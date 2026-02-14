use anyhow::{anyhow, Result};
use futures::{StreamExt, SinkExt};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::models::{CoinbaseMatch, CoinbaseSubscribe};
use crate::types::{Exchange, Market, Symbol};
use crate::aggregator::{BucketAgg, SecAgg};
use crate::AppState;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;

pub async fn run_coinbase_engine(
    state: &AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    sh: &crate::Shard,
    mut write: futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>,
    mut read: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
) -> Result<()> {
    let product_ids = match symbol {
        Symbol::Btc => vec!["BTC-USD"],
        Symbol::Eth => vec!["ETH-USD"],
        _ => return Err(anyhow!("Coinbase only supports BTC and ETH")),
    };

    let sub = CoinbaseSubscribe {
        msg_type: "subscribe",
        product_ids,
        channels: vec!["matches"],
    };
    write
        .send(Message::Text(
            serde_json::to_string(&sub)?,
        ))
        .await?;

    let mut cvd_closed_usdt = *sh.cvd_closed_usdt.read().await;
    let mut sec = SecAgg::new();
    let mut a1 = BucketAgg::new();
    let mut a5 = BucketAgg::new();

    let mut ping = tokio::time::interval(Duration::from_secs(crate::types::COINBASE_PING_INTERVAL_SECS));
    loop {
        tokio::select! {
            _ = ping.tick() => {
                // Keep-alive ping
                let _ = write.send(
                    Message::Ping(vec![])
                ).await;
            }

            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                };

                match msg {
                    Message::Ping(payload) => {
                        write
                            .send(Message::Pong(payload))
                            .await?;
                    }
                    Message::Text(txt) => {
                        let m: CoinbaseMatch = match serde_json::from_str(&txt) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if m.msg_type != "match" {
                            continue;
                        }
                        let expected_product = match symbol {
                            Symbol::Btc => "BTC-USD",
                            Symbol::Eth => "ETH-USD",
                            _ => unreachable!(),
                        };
                        if m.product_id != expected_product {
                            continue;
                        }

                        // side: "buy" (taker buy) => positive, "sell" (taker sell) => negative
                        let signed_size = if m.side == "sell" { m.size } else { -m.size };
                        let delta_usdt = signed_size * m.price;

                        let ts_ms = crate::parse_iso8601_to_ms(&m.time)?;
                        let this_sec = ts_ms / 1000;

                        match sec.cur_sec {
                            None => sec.reset_to(this_sec, m.price, delta_usdt),
                            Some(s) if s != this_sec => {
                                let sec_ended = s;
                                let sec_last_price = sec.last_price;
                                let sec_delta = sec.delta_usdt;
                                let sec_trades = sec.trades;

                                crate::flush_second(
                                    state,
                                    exchange,
                                    market,
                                    symbol,
                                    &mut a1,
                                    &mut a5,
                                    &mut cvd_closed_usdt,
                                    sec_ended,
                                    sec_last_price,
                                    sec_delta,
                                    sec_trades,
                                )
                                .await;

                                *sh.cvd_closed_usdt.write().await = cvd_closed_usdt;

                                sec.reset_to(this_sec, m.price, delta_usdt);
                            }
                            _ => sec.add(m.price, delta_usdt),
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }

    // flush last partial second (best-effort)
    if let Some(s) = sec.cur_sec {
        crate::flush_second(
            state,
            exchange,
            market,
            symbol,
            &mut a1,
            &mut a5,
            &mut cvd_closed_usdt,
            s,
            sec.last_price,
            sec.delta_usdt,
            sec.trades,
        )
        .await;

        *sh.cvd_closed_usdt.write().await = cvd_closed_usdt;
    }

    Err(anyhow!("ws ended"))
}

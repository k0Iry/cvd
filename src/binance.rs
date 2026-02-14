use anyhow::{anyhow, Result};
use futures::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::Message;

use crate::models::AggTrade;
use crate::types::{Exchange, Market, Symbol};
use crate::aggregator::{BucketAgg, SecAgg};
use crate::AppState;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;

pub async fn run_binance_engine(
    state: &AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    sh: &crate::Shard,
    mut write: futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>,
    mut read: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
) -> Result<()> {
    let mut cvd_closed_usdt = *sh.cvd_closed_usdt.read().await;

    let mut sec = SecAgg::new();
    let mut a1 = BucketAgg::new();
    let mut a5 = BucketAgg::new();

    while let Some(msg) = read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[{}:{}:{}] read err: {e}", exchange.as_str(), market.as_str(), symbol.as_str());
                break;
            }
        };

        match msg {
            Message::Ping(payload) => {
                write
                    .send(Message::Pong(payload))
                    .await?;
            }
            Message::Text(txt) => {
                let t: AggTrade = match serde_json::from_str(&txt) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if t.event_type != "aggTrade" {
                    continue;
                }

                let price: f64 = match t.price.parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let qty: f64 = match t.quantity.parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // m=true => buyer is maker => aggressive SELL => negative delta
                let delta_base = if t.is_buyer_maker { -qty } else { qty };
                let delta_usdt = delta_base * price;

                let ts_ms = t.trade_time;
                let this_sec = ts_ms / 1000;

                match sec.cur_sec {
                    None => sec.reset_to(this_sec, price, delta_usdt),
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

                        sec.reset_to(this_sec, price, delta_usdt);
                    }
                    _ => sec.add(price, delta_usdt),
                }
            }
            Message::Close(_) => break,
            _ => {}
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

use futures::{SinkExt, StreamExt};
use rand::Rng;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";

// Binance aggTrade payload (fields we need)
#[derive(Debug, Deserialize)]
struct AggTrade {
    #[serde(rename = "e")]
    event_type: String,

    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "a")]
    agg_trade_id: u64,

    // price/qty are strings in Binance payload; parse to f64 ourselves
    #[serde(rename = "p")]
    price: String,

    #[serde(rename = "q")]
    quantity: String,

    #[serde(rename = "T")]
    trade_time: i64,

    // true => buyer is maker => aggressive side is SELL (market sell)
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

/// Compute signed delta from aggTrade:
/// m=true  => market sell => -qty
/// m=false => market buy  => +qty
fn trade_delta(qty: f64, is_buyer_maker: bool) -> f64 {
    if is_buyer_maker {
        -qty
    } else {
        qty
    }
}

/// Exponential backoff with jitter, capped.
fn backoff_with_jitter(attempt: u32) -> Duration {
    // base grows: 1s, 2s, 4s, 8s, ... up to 60s
    let base_secs = (1u64 << attempt.min(6)) as u64; // 1..64
    let capped = base_secs.min(60);

    // jitter: 0..250ms
    let jitter_ms: u64 = rand::thread_rng().gen_range(0..=250);

    Duration::from_secs(capped) + Duration::from_millis(jitter_ms)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to: {WS_URL}");
    println!("Rule: m=true => market sell (-qty), m=false => market buy (+qty)");
    println!("Writing: facts_1s.csv (append-only)\n");

    let mut attempt: u32 = 0;
    loop {
        match run_once().await {
            Ok(_) => attempt = 0,
            Err(err) => eprintln!("run_once error: {err}"),
        }

        attempt = attempt.saturating_add(1);
        let wait = backoff_with_jitter(attempt);
        eprintln!(
            "Reconnecting in {:.3}s (attempt={attempt})...",
            wait.as_secs_f64()
        );
        sleep(wait).await;
    }
}

async fn run_once() -> anyhow::Result<()> {
    // Open CSV in append mode
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("facts_1s.csv")?;

    // Write header if empty
    if file.metadata()?.len() == 0 {
        writeln!(
            file,
            "t_sec,t_end_ms,price_close,delta_btc_1s,delta_usdt_1s,cvd_btc,cvd_usdt,trades"
        )?;
        file.flush()?;
    }

    let (ws_stream, _resp) = connect_async(WS_URL).await?;
    let (mut write, mut read) = ws_stream.split();

    // CVD state (cumulative since process start)
    let mut cvd_btc: f64 = 0.0;
    let mut cvd_usdt: f64 = 0.0;

    // 1-second bucket state
    let mut cur_sec: Option<i64> = None;
    let mut sec_delta_btc: f64 = 0.0;
    let mut sec_trades: u64 = 0;
    let mut sec_last_price: f64 = 0.0;

    while let Some(msg) = read.next().await {
        let msg = msg?;

        match msg {
            Message::Ping(payload) => {
                // Binance requires: pong with exact payload, asap
                write.send(Message::Pong(payload)).await?;
            }
            Message::Pong(_) => {}
            Message::Text(txt) => {
                let trade: AggTrade = match serde_json::from_str(&txt) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if trade.event_type != "aggTrade" {
                    continue;
                }

                let qty = match parse_f64(&trade.quantity) {
                    Some(v) => v,
                    None => continue,
                };
                let price = match parse_f64(&trade.price) {
                    Some(v) => v,
                    None => continue,
                };

                let delta_btc = trade_delta(qty, trade.is_buyer_maker);
                let sec = trade.trade_time / 1000;

                // rollover check
                match cur_sec {
                    None => {
                        cur_sec = Some(sec);
                    }
                    Some(s) if s != sec => {
                        // close previous second -> compute delta_usdt using last price of that second
                        let sec_delta_usdt = sec_delta_btc * sec_last_price;

                        // update cumulative CVD at second boundary
                        cvd_btc += sec_delta_btc;
                        cvd_usdt += sec_delta_usdt;

                        // write one CSV row (append-only)
                        let t_end_ms = (s + 1) * 1000 - 1;
                        writeln!(
                            file,
                            "{},{},{:.2},{:+.8},{:+.2},{:+.8},{:+.2},{}",
                            s,
                            t_end_ms,
                            sec_last_price,
                            sec_delta_btc,
                            sec_delta_usdt,
                            cvd_btc,
                            cvd_usdt,
                            sec_trades
                        )?;
                        file.flush()?; // so python can tail reliably

                        // reset for new second
                        cur_sec = Some(sec);
                        sec_delta_btc = 0.0;
                        sec_trades = 0;
                        // sec_last_price will be set below by current trade
                    }
                    _ => {}
                }

                // accumulate into current second bucket
                sec_delta_btc += delta_btc;
                sec_trades += 1;
                sec_last_price = price;
            }
            Message::Close(frame) => {
                eprintln!("WS closed by server: {:?}", frame);
                break;
            }
            _ => {}
        }
    }

    Err(anyhow::anyhow!("ws stream ended"))
}

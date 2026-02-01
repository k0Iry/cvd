use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{Query, State, WebSocketUpgrade},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::str::FromStr;
use std::{collections::VecDeque, net::SocketAddr, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tower_http::services::ServeDir;

const BINANCE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";

#[derive(Debug, Deserialize)]
struct AggTrade {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    trade_time: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Debug, Clone, Serialize)]
struct Bar {
    // unix seconds, good for lightweight-charts
    time: i64,
    price_close: f64,
    delta_usdt: f64,
    cvd_usdt: f64,
    trades: u64,
}

#[derive(Clone, Copy)]
enum Tf {
    M1,
    M5,
}
impl Tf {
    fn ms(self) -> i64 {
        match self {
            Tf::M1 => 60_000,
            Tf::M5 => 300_000,
        }
    }
    fn from_str(s: &str) -> Tf {
        match s {
            "5m" => Tf::M5,
            _ => Tf::M1,
        }
    }
}

#[derive(Debug, Clone)]
struct BucketAgg {
    cur_bucket_id: Option<i64>,
    last_price: f64,
    delta_usdt: f64,
    trades: u64,
}
impl BucketAgg {
    fn new() -> Self {
        Self {
            cur_bucket_id: None,
            last_price: f64::NAN,
            delta_usdt: 0.0,
            trades: 0,
        }
    }
}

#[derive(Clone)]
struct AppState {
    hist_1m: Arc<RwLock<VecDeque<Bar>>>,
    hist_5m: Arc<RwLock<VecDeque<Bar>>>,
    tx_1m: broadcast::Sender<Bar>,
    tx_5m: broadcast::Sender<Bar>,

    db: SqlitePool,
    cvd_closed_usdt: Arc<RwLock<f64>>, // <--- 新增：跨断线/重启保持
}

#[derive(sqlx::FromRow)]
struct BarRow {
    time: i64,
    price_close: f64,
    delta_usdt: f64,
    cvd_usdt: f64,
    trades: i64,
}

#[derive(Deserialize)]
struct Params {
    tf: Option<String>,
    limit: Option<usize>,
}

async fn init_db(db: &SqlitePool) -> Result<()> {
    sqlx::query("PRAGMA journal_mode=WAL;").execute(db).await?;
    sqlx::query("PRAGMA synchronous=NORMAL;")
        .execute(db)
        .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bars (
            tf TEXT NOT NULL,
            time INTEGER NOT NULL,
            price_close REAL NOT NULL,
            delta_usdt REAL NOT NULL,
            cvd_usdt REAL NOT NULL,
            trades INTEGER NOT NULL,
            PRIMARY KEY (tf, time)
        );
        "#,
    )
    .execute(db)
    .await?;

    Ok(())
}

async fn load_history(db: &SqlitePool, tf: &str, limit: usize) -> Result<(VecDeque<Bar>, f64)> {
    let rows: Vec<BarRow> = sqlx::query_as(
        r#"
    SELECT time, price_close, delta_usdt, cvd_usdt, trades
    FROM bars
    WHERE tf = ?
    ORDER BY time DESC
    LIMIT ?
    "#,
    )
    .bind(tf)
    .bind(limit as i64)
    .fetch_all(db)
    .await?;

    // rows are DESC -> reverse to ASC
    let mut v: Vec<Bar> = rows
        .into_iter()
        .map(|r| Bar {
            time: r.time,
            price_close: r.price_close,
            delta_usdt: r.delta_usdt,
            cvd_usdt: r.cvd_usdt,
            trades: r.trades as u64,
        })
        .collect();
    v.reverse();

    let last_cvd = v.last().map(|b| b.cvd_usdt).unwrap_or(0.0);
    Ok((VecDeque::from(v), last_cvd))
}

#[tokio::main]
async fn main() -> Result<()> {
    let (tx_1m, _) = broadcast::channel::<Bar>(2048);
    let (tx_5m, _) = broadcast::channel::<Bar>(2048);

    let opts = SqliteConnectOptions::from_str("sqlite:cvd.db")?.create_if_missing(true);

    let db = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await?;

    init_db(&db).await?;

    // 启动时从 DB 恢复历史（也会恢复 last_cvd）
    let (hist_1m, last_cvd_1m) = load_history(&db, "1m", 2000).await?;
    let (hist_5m, _last_cvd_5m) = load_history(&db, "5m", 2000).await?;

    let state = AppState {
        hist_1m: Arc::new(RwLock::new(hist_1m)),
        hist_5m: Arc::new(RwLock::new(hist_5m)),
        tx_1m,
        tx_5m,
        db,
        cvd_closed_usdt: Arc::new(RwLock::new(last_cvd_1m)),
    };

    // spawn engine
    tokio::spawn(engine_loop(state.clone()));

    // axum app
    let app = Router::new()
        .route("/history", get(history))
        .route("/ws", get(ws_upgrade))
        // serve static UI (index.html) at /
        .nest_service("/", ServeDir::new("static"))
        .with_state(state);

    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    println!("Open UI: http://{addr}/");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;

    Ok(())
}

async fn history(State(state): State<AppState>, Query(p): Query<Params>) -> impl IntoResponse {
    let tf = Tf::from_str(p.tf.as_deref().unwrap_or("1m"));
    let limit = p.limit.unwrap_or(500).min(2000);

    let bars: Vec<Bar> = match tf {
        Tf::M1 => {
            let h = state.hist_1m.read().await;
            h.iter()
                .rev()
                .take(limit)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect()
        }
        Tf::M5 => {
            let h = state.hist_5m.read().await;
            h.iter()
                .rev()
                .take(limit)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect()
        }
    };

    Json(bars)
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(p): Query<Params>,
) -> impl IntoResponse {
    let tf = Tf::from_str(p.tf.as_deref().unwrap_or("1m"));
    ws.on_upgrade(move |socket| ws_handler(socket, state, tf))
}

async fn ws_handler(mut socket: WebSocket, state: AppState, tf: Tf) {
    let mut rx = match tf {
        Tf::M1 => state.tx_1m.subscribe(),
        Tf::M5 => state.tx_5m.subscribe(),
    };

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    None => {
                        // client disconnected
                        break;
                    }
                    Some(Ok(Message::Close(_))) => {
                        break;
                    }
                    Some(Ok(_)) => {
                        // ignore other inbound messages
                    }
                    Some(Err(_e)) => {
                        break;
                    }
                }
            }

            msg = rx.recv() => {
                match msg {
                    Ok(bar) => {
                        let Ok(txt) = serde_json::to_string(&bar) else { continue; };
                        if socket.send(Message::Text(txt)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // client fell behind; just continue
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

async fn engine_loop(state: AppState) {
    let mut attempt: u32 = 0;
    loop {
        match run_engine_once(&state).await {
            Ok(_) => attempt = 0,
            Err(e) => {
                if e.to_string().contains("ws ended") {
                    attempt = 0; // 这是正常断开/结束
                } else {
                    eprintln!("engine error: {e}");
                    attempt = attempt.saturating_add(1);
                }
            }
        }

        let wait = backoff_with_jitter(attempt);
        sleep(wait).await;
    }
}

#[derive(Debug, Clone)]
struct SecAgg {
    cur_sec: Option<i64>, // unix seconds
    last_price: f64,
    delta_usdt: f64,
    trades: u64,
}

impl SecAgg {
    fn new() -> Self {
        Self {
            cur_sec: None,
            last_price: f64::NAN,
            delta_usdt: 0.0,
            trades: 0,
        }
    }

    fn reset_to(&mut self, sec: i64, price: f64, delta_usdt: f64) {
        self.cur_sec = Some(sec);
        self.last_price = price;
        self.delta_usdt = delta_usdt;
        self.trades = 1;
    }

    fn add(&mut self, price: f64, delta_usdt: f64) {
        self.last_price = price;
        self.delta_usdt += delta_usdt;
        self.trades += 1;
    }
}

async fn flush_second(
    state: &AppState,
    a1: &mut BucketAgg,
    a5: &mut BucketAgg,
    cvd_closed_usdt: &mut f64,
    sec_ended: i64,
    sec_last_price: f64,
    sec_delta_usdt: f64,
    sec_trades: u64,
) {
    // end-of-second timestamp in ms (keeps it in correct minute)
    let ts_ms_flush = sec_ended * 1000 + 999;

    // 1m close-only (only when minute rolls)
    if let Some(mut closed_bar_1m) =
        push_bucket(a1, Tf::M1, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        *cvd_closed_usdt += closed_bar_1m.delta_usdt;
        closed_bar_1m.cvd_usdt = *cvd_closed_usdt;

        // 写回全局（跨断线/重启连续）
        *state.cvd_closed_usdt.write().await = *cvd_closed_usdt;

        push_publish(state, Tf::M1, closed_bar_1m).await;
    }

    // 1m live (every second)
    let tf_ms = Tf::M1.ms();
    let cur_bucket_id = ts_ms_flush / tf_ms;
    let cur_end_ms = (cur_bucket_id + 1) * tf_ms - 1;

    let live_bar_1m = Bar {
        time: cur_end_ms / 1000,
        price_close: a1.last_price,
        delta_usdt: a1.delta_usdt,
        cvd_usdt: *cvd_closed_usdt + a1.delta_usdt,
        trades: a1.trades, // 注意：现在是“秒tick数”，不是原始成交笔数
    };
    let _ = state.tx_1m.send(live_bar_1m);

    // 5m close-only（可选）
    if let Some(mut closed_bar_5m) =
        push_bucket(a5, Tf::M5, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        closed_bar_5m.cvd_usdt = *cvd_closed_usdt;
        push_publish(state, Tf::M5, closed_bar_5m).await;
    }

    let _ = sec_trades; // 先占位，避免 unused warning（如果你暂时不用它）
}

async fn run_engine_once(state: &AppState) -> Result<()> {
    let (ws_stream, _resp) = connect_async(BINANCE_WS).await?;
    let (mut write, mut read) = ws_stream.split();

    // cumulative CVD for CLOSED 1m bars
    let mut cvd_closed_usdt = *state.cvd_closed_usdt.read().await;

    // 1-second aggregator
    let mut sec = SecAgg::new();

    // 1m/5m bucket aggregators (but now they consume 1s-flushed events)
    let mut a1 = BucketAgg::new();
    let mut a5 = BucketAgg::new();

    while let Some(msg) = read.next().await {
        let msg = msg?;

        match msg {
            tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                write
                    .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                    .await?;
            }
            tokio_tungstenite::tungstenite::Message::Text(txt) => {
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

                // m=true => market sell => negative delta
                let delta_btc = if t.is_buyer_maker { -qty } else { qty };
                let delta_usdt = delta_btc * price;

                let ts_ms = t.trade_time;
                let this_sec = ts_ms / 1000;

                match sec.cur_sec {
                    None => {
                        sec.reset_to(this_sec, price, delta_usdt);
                    }
                    Some(s) if s != this_sec => {
                        // flush the finished second (s) into 1m/5m, then start new second
                        let sec_ended = s;
                        let sec_last_price = sec.last_price;
                        let sec_delta = sec.delta_usdt;
                        let sec_trades = sec.trades;

                        flush_second(
                            state,
                            &mut a1,
                            &mut a5,
                            &mut cvd_closed_usdt,
                            sec_ended,
                            sec_last_price,
                            sec_delta,
                            sec_trades,
                        )
                        .await;

                        sec.reset_to(this_sec, price, delta_usdt);
                    }
                    _ => {
                        // same second
                        sec.add(price, delta_usdt);
                    }
                }
            }
            tokio_tungstenite::tungstenite::Message::Close(_) => break,
            _ => {}
        }
    }

    // flush last partial second (best-effort)
    if let Some(s) = sec.cur_sec {
        flush_second(
            state,
            &mut a1,
            &mut a5,
            &mut cvd_closed_usdt,
            s,
            sec.last_price,
            sec.delta_usdt,
            sec.trades,
        )
        .await;
    }
    Err(anyhow::anyhow!("ws ended"))
}

fn push_bucket(
    agg: &mut BucketAgg,
    tf: Tf,
    ts_ms: i64,
    price: f64,
    delta_usdt: f64,
) -> Option<Bar> {
    let tf_ms = tf.ms();
    let bucket_id = ts_ms / tf_ms;

    match agg.cur_bucket_id {
        None => agg.cur_bucket_id = Some(bucket_id),
        Some(cur) if cur != bucket_id => {
            // close previous bucket
            if agg.trades == 0 || agg.last_price.is_nan() {
                // defensive
                agg.cur_bucket_id = Some(bucket_id);
                agg.last_price = price;
                agg.delta_usdt = delta_usdt;
                agg.trades = 1;
                return None;
            }

            let end_ms = (cur + 1) * tf_ms - 1;
            let end_sec = end_ms / 1000;

            let bar = Bar {
                time: end_sec,
                price_close: agg.last_price,
                delta_usdt: agg.delta_usdt,
                cvd_usdt: 0.0, // filled by caller
                trades: agg.trades,
            };

            // start new bucket with current trade
            agg.cur_bucket_id = Some(bucket_id);
            agg.last_price = price;
            agg.delta_usdt = delta_usdt;
            agg.trades = 1;

            return Some(bar);
        }
        _ => {}
    }

    // same bucket
    agg.last_price = price;
    agg.delta_usdt += delta_usdt;
    agg.trades += 1;

    None
}

async fn push_publish(state: &AppState, tf: Tf, bar: Bar) {
    let max = 2000usize;

    let tf_str = match tf {
        Tf::M1 => "1m",
        Tf::M5 => "5m",
    };

    // 1) persist (UPSERT)
    let _ = sqlx::query(
        r#"
    INSERT INTO bars (tf, time, price_close, delta_usdt, cvd_usdt, trades)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(tf, time) DO UPDATE SET
        price_close=excluded.price_close,
        delta_usdt=excluded.delta_usdt,
        cvd_usdt=excluded.cvd_usdt,
        trades=excluded.trades
    "#,
    )
    .bind(tf_str)
    .bind(bar.time)
    .bind(bar.price_close)
    .bind(bar.delta_usdt)
    .bind(bar.cvd_usdt)
    .bind(bar.trades as i64)
    .execute(&state.db)
    .await;

    // 2) in-memory history + ws broadcast
    match tf {
        Tf::M1 => {
            let mut h = state.hist_1m.write().await;
            if h.len() >= max {
                h.pop_front();
            }
            h.push_back(bar.clone());
            let _ = state.tx_1m.send(bar);
        }
        Tf::M5 => {
            let mut h = state.hist_5m.write().await;
            if h.len() >= max {
                h.pop_front();
            }
            h.push_back(bar.clone());
            let _ = state.tx_5m.send(bar);
        }
    }
}

fn backoff_with_jitter(attempt: u32) -> Duration {
    let base = (1u64 << attempt.min(6)).min(60);
    let jitter_ms: u64 = rand::thread_rng().gen_range(0..=250);
    Duration::from_secs(base) + Duration::from_millis(jitter_ms)
}

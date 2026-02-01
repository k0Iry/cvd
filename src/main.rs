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
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;
use std::{collections::VecDeque, net::SocketAddr, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tower_http::services::ServeDir;

// -------- Binance markets --------

#[derive(Clone, Copy, Debug)]
enum Market {
    Spot,
    Usdm,
    Coinm,
}

impl Market {
    fn from_str(s: &str) -> Self {
        match s {
            "usdm" => Self::Usdm,
            "coinm" => Self::Coinm,
            _ => Self::Spot,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Usdm => "usdm",
            Self::Coinm => "coinm",
        }
    }

    fn ws_url(self) -> &'static str {
        match self {
            // Spot BTCUSDT
            Self::Spot => "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
            // USDⓈ-M futures BTCUSDT
            Self::Usdm => "wss://fstream.binance.com/ws/btcusdt@aggTrade",
            // COIN-M futures BTCUSD_PERP (stream uses lowercase)
            Self::Coinm => "wss://dstream.binance.com/ws/btcusd_perp@aggTrade",
        }
    }
}

// -------- Models --------

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
    // unix seconds, used by lightweight-charts
    time: i64,
    price_close: f64,
    delta_usdt: f64,
    cvd_usdt: f64,
    trades: u64,
}

#[derive(sqlx::FromRow)]
struct BarRow {
    time: i64,
    price_close: f64,
    delta_usdt: f64,
    cvd_usdt: f64,
    trades: i64,
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
    fn as_str(self) -> &'static str {
        match self {
            Tf::M1 => "1m",
            Tf::M5 => "5m",
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

// -------- State --------

#[derive(Clone)]
struct MarketState {
    hist_1m: Arc<RwLock<VecDeque<Bar>>>,
    hist_5m: Arc<RwLock<VecDeque<Bar>>>,
    tx_1m: broadcast::Sender<Bar>,
    tx_5m: broadcast::Sender<Bar>,
    cvd_closed_usdt: Arc<RwLock<f64>>,
}

fn mk_market_state() -> MarketState {
    let (tx_1m, _) = broadcast::channel::<Bar>(2048);
    let (tx_5m, _) = broadcast::channel::<Bar>(2048);
    MarketState {
        hist_1m: Arc::new(RwLock::new(VecDeque::with_capacity(4000))),
        hist_5m: Arc::new(RwLock::new(VecDeque::with_capacity(4000))),
        tx_1m,
        tx_5m,
        cvd_closed_usdt: Arc::new(RwLock::new(0.0)),
    }
}

#[derive(Clone)]
struct AppState {
    spot: MarketState,
    usdm: MarketState,
    coinm: MarketState,
    db: SqlitePool,
}

fn pick(s: &AppState, m: Market) -> &MarketState {
    match m {
        Market::Spot => &s.spot,
        Market::Usdm => &s.usdm,
        Market::Coinm => &s.coinm,
    }
}

fn pick_owned(s: &AppState, m: Market) -> MarketState {
    match m {
        Market::Spot => s.spot.clone(),
        Market::Usdm => s.usdm.clone(),
        Market::Coinm => s.coinm.clone(),
    }
}

// -------- HTTP params --------

#[derive(Deserialize)]
struct Params {
    market: Option<String>,
    tf: Option<String>,
    limit: Option<usize>,
}

// -------- DB --------

async fn init_db(db: &SqlitePool) -> Result<()> {
    sqlx::query("PRAGMA journal_mode=WAL;").execute(db).await?;
    sqlx::query("PRAGMA synchronous=NORMAL;")
        .execute(db)
        .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bars (
            market TEXT NOT NULL,
            tf TEXT NOT NULL,
            time INTEGER NOT NULL,
            price_close REAL NOT NULL,
            delta_usdt REAL NOT NULL,
            cvd_usdt REAL NOT NULL,
            trades INTEGER NOT NULL,
            PRIMARY KEY (market, tf, time)
        );
        "#,
    )
    .execute(db)
    .await?;

    Ok(())
}

async fn load_history(
    db: &SqlitePool,
    market: &str,
    tf: &str,
    limit: usize,
) -> Result<(VecDeque<Bar>, f64)> {
    let rows: Vec<BarRow> = sqlx::query_as(
        r#"
        SELECT time, price_close, delta_usdt, cvd_usdt, trades
        FROM bars
        WHERE market = ? AND tf = ?
        ORDER BY time DESC
        LIMIT ?
        "#,
    )
    .bind(market)
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

async fn hydrate_market(state: &AppState, market: Market) -> Result<()> {
    let m = market.as_str();
    let (h1, last1) = load_history(&state.db, m, "1m", 2000).await?;
    let (h5, _last5) = load_history(&state.db, m, "5m", 2000).await?;

    let ms = pick(state, market);
    *ms.hist_1m.write().await = h1;
    *ms.hist_5m.write().await = h5;
    *ms.cvd_closed_usdt.write().await = last1;

    Ok(())
}

// -------- Main --------

#[tokio::main]
async fn main() -> Result<()> {
    // DB URL from env (default relative file)
    let sqlite_url = std::env::var("SQLITE_URL").unwrap_or_else(|_| "sqlite:cvd.db".to_string());
    let opts = SqliteConnectOptions::from_str(&sqlite_url)?.create_if_missing(true);

    let db = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await?;

    init_db(&db).await?;

    let state = AppState {
        spot: mk_market_state(),
        usdm: mk_market_state(),
        coinm: mk_market_state(),
        db,
    };

    // load histories + cvd_closed for each market
    hydrate_market(&state, Market::Spot).await?;
    hydrate_market(&state, Market::Usdm).await?;
    hydrate_market(&state, Market::Coinm).await?;

    // start 3 engines concurrently
    tokio::spawn(engine_loop(state.clone(), Market::Spot));
    tokio::spawn(engine_loop(state.clone(), Market::Usdm));
    tokio::spawn(engine_loop(state.clone(), Market::Coinm));

    // axum app
    let app = Router::new()
        .route("/history", get(history))
        .route("/ws", get(ws_upgrade))
        .nest_service("/", ServeDir::new("static"))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8000".parse().unwrap();
    println!("Open UI: http://{addr}/");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;

    Ok(())
}

// -------- HTTP handlers --------

async fn history(State(state): State<AppState>, Query(p): Query<Params>) -> impl IntoResponse {
    let market = Market::from_str(p.market.as_deref().unwrap_or("spot"));
    let ms = pick(&state, market);

    let tf = Tf::from_str(p.tf.as_deref().unwrap_or("1m"));
    let limit = p.limit.unwrap_or(500).min(2000);

    let bars: Vec<Bar> = match tf {
        Tf::M1 => {
            let h = ms.hist_1m.read().await;
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
            let h = ms.hist_5m.read().await;
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
    let market = Market::from_str(p.market.as_deref().unwrap_or("spot"));
    let tf = Tf::from_str(p.tf.as_deref().unwrap_or("1m"));
    ws.on_upgrade(move |socket| ws_handler(socket, state, market, tf))
}

async fn ws_handler(mut socket: WebSocket, state: AppState, market: Market, tf: Tf) {
    let ms = pick_owned(&state, market);

    let mut rx = match tf {
        Tf::M1 => ms.tx_1m.subscribe(),
        Tf::M5 => ms.tx_5m.subscribe(),
    };

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    None => break,
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
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
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        }
    }
}

// -------- Engine --------

async fn engine_loop(state: AppState, market: Market) {
    let mut attempt: u32 = 0;

    loop {
        let res = run_engine_once(&state, market).await;

        match res {
            Ok(_) => {
                // ended gracefully, immediate retry
                attempt = 0;
            }
            Err(e) => {
                // treat ws ended as "normal reconnect", don't let backoff explode
                let s = e.to_string();
                if s.contains("ws ended") {
                    attempt = 0;
                } else {
                    eprintln!("[{}] engine error: {e}", market.as_str());
                    attempt = attempt.saturating_add(1);
                }
            }
        }

        let wait = backoff_with_jitter(attempt);
        eprintln!(
            "[{}] reconnecting in {:.3}s (attempt={})",
            market.as_str(),
            wait.as_secs_f64(),
            attempt
        );
        sleep(wait).await;
    }
}

async fn run_engine_once(state: &AppState, market: Market) -> Result<()> {
    let url = market.ws_url();
    eprintln!("[{}] Connecting to: {}", market.as_str(), url);

    let (ws_stream, _resp) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    // market-specific state
    let ms = pick(state, market);

    // cumulative CVD for CLOSED 1m bars
    let mut cvd_closed_usdt = *ms.cvd_closed_usdt.read().await;

    // 1-second aggregator
    let mut sec = SecAgg::new();

    // 1m/5m bucket aggregators consuming 1s flush events
    let mut a1 = BucketAgg::new();
    let mut a5 = BucketAgg::new();

    while let Some(msg) = read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[{}] read err: {e}", market.as_str());
                break;
            }
        };

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

                // m=true => buyer is maker => aggressive SELL => negative delta
                let delta_btc = if t.is_buyer_maker { -qty } else { qty };
                let delta_usdt = delta_btc * price;

                let ts_ms = t.trade_time;
                let this_sec = ts_ms / 1000;

                match sec.cur_sec {
                    None => sec.reset_to(this_sec, price, delta_usdt),
                    Some(s) if s != this_sec => {
                        // flush finished second (s), then start new one
                        let sec_ended = s;
                        let sec_last_price = sec.last_price;
                        let sec_delta = sec.delta_usdt;
                        let sec_trades = sec.trades;

                        flush_second(
                            state,
                            market,
                            &mut a1,
                            &mut a5,
                            &mut cvd_closed_usdt,
                            sec_ended,
                            sec_last_price,
                            sec_delta,
                            sec_trades,
                        )
                        .await;

                        // persist cvd_closed back to market state
                        *ms.cvd_closed_usdt.write().await = cvd_closed_usdt;

                        sec.reset_to(this_sec, price, delta_usdt);
                    }
                    _ => sec.add(price, delta_usdt),
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
            market,
            &mut a1,
            &mut a5,
            &mut cvd_closed_usdt,
            s,
            sec.last_price,
            sec.delta_usdt,
            sec.trades,
        )
        .await;

        *ms.cvd_closed_usdt.write().await = cvd_closed_usdt;
    }

    Err(anyhow::anyhow!("ws ended"))
}

async fn flush_second(
    state: &AppState,
    market: Market,
    a1: &mut BucketAgg,
    a5: &mut BucketAgg,
    cvd_closed_usdt: &mut f64,
    sec_ended: i64,
    sec_last_price: f64,
    sec_delta_usdt: f64,
    _sec_trades: u64,
) {
    // keep it in correct minute bucket: use end-of-second ms
    let ts_ms_flush = sec_ended * 1000 + 999;

    // 1m close-only
    if let Some(mut closed_bar_1m) =
        push_bucket(a1, Tf::M1, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        *cvd_closed_usdt += closed_bar_1m.delta_usdt;
        closed_bar_1m.cvd_usdt = *cvd_closed_usdt;
        push_publish(state, market, Tf::M1, closed_bar_1m).await;
    }

    // 1m live every second (update last point)
    let tf_ms = Tf::M1.ms();
    let cur_bucket_id = ts_ms_flush / tf_ms;
    let cur_end_ms = (cur_bucket_id + 1) * tf_ms - 1;

    let live_bar_1m = Bar {
        time: cur_end_ms / 1000,
        price_close: a1.last_price,
        delta_usdt: a1.delta_usdt,
        cvd_usdt: *cvd_closed_usdt + a1.delta_usdt,
        trades: a1.trades, // seconds-in-bucket (not raw trade count)
    };
    let ms = pick(state, market);
    let _ = ms.tx_1m.send(live_bar_1m);

    // 5m close-only
    if let Some(mut closed_bar_5m) =
        push_bucket(a5, Tf::M5, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        closed_bar_5m.cvd_usdt = *cvd_closed_usdt;
        push_publish(state, market, Tf::M5, closed_bar_5m).await;
    }
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
                // defensive: start a new bucket
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

            // start new bucket with current tick
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

async fn push_publish(state: &AppState, market: Market, tf: Tf, bar: Bar) {
    let tf_str = tf.as_str();
    let market_str = market.as_str();

    // 1) persist (UPSERT)
    if let Err(e) = sqlx::query(
        r#"
        INSERT INTO bars (market, tf, time, price_close, delta_usdt, cvd_usdt, trades)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, tf, time) DO UPDATE SET
            price_close=excluded.price_close,
            delta_usdt=excluded.delta_usdt,
            cvd_usdt=excluded.cvd_usdt,
            trades=excluded.trades
        "#,
    )
    .bind(market_str)
    .bind(tf_str)
    .bind(bar.time)
    .bind(bar.price_close)
    .bind(bar.delta_usdt)
    .bind(bar.cvd_usdt)
    .bind(bar.trades as i64)
    .execute(&state.db)
    .await
    {
        eprintln!(
            "db write failed market={} tf={} time={} err={e}",
            market_str, tf_str, bar.time
        );
    }

    // 2) in-memory history + ws broadcast for CLOSE bars
    let ms = pick(state, market);
    let max = 2000usize;

    match tf {
        Tf::M1 => {
            let mut h = ms.hist_1m.write().await;
            if h.len() >= max {
                h.pop_front();
            }
            h.push_back(bar.clone());
            let _ = ms.tx_1m.send(bar);
        }
        Tf::M5 => {
            let mut h = ms.hist_5m.write().await;
            if h.len() >= max {
                h.pop_front();
            }
            h.push_back(bar.clone());
            let _ = ms.tx_5m.send(bar);
        }
    }
}

fn backoff_with_jitter(attempt: u32) -> Duration {
    // attempt==0 => 1s; grows to max 60s
    let exp = (1u64 << attempt.min(6)).min(60);
    let base = exp.max(1);
    let jitter_ms: u64 = rand::thread_rng().gen_range(0..=250);
    Duration::from_secs(base) + Duration::from_millis(jitter_ms)
}

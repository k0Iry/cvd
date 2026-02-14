use anyhow::{anyhow, Result};
use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{Query, State, WebSocketUpgrade},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::hash::Hash;
use std::str::FromStr;
use std::{collections::VecDeque, net::SocketAddr, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tower_http::services::ServeDir;

// ==============================
// Types: Exchange / Market / Symbol / Tf
// ==============================

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Exchange {
    Binance,
    Coinbase,
}
impl Exchange {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "coinbase" => Self::Coinbase,
            _ => Self::Binance,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Coinbase => "coinbase",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Symbol {
    BTC,
    ETH,
    SOL,
    BNB,
    AAVE,
}
impl Symbol {
    fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ETH" => Self::ETH,
            "SOL" => Self::SOL,
            "BNB" => Self::BNB,
            "AAVE" => Self::AAVE,
            _ => Self::BTC,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::BTC => "BTC",
            Self::ETH => "ETH",
            Self::SOL => "SOL",
            Self::BNB => "BNB",
            Self::AAVE => "AAVE",
        }
    }

    // Spot / USDM use <asset>USDT
    fn spot_usdm_stream(self) -> &'static str {
        match self {
            Self::BTC => "btcusdt",
            Self::ETH => "ethusdt",
            Self::SOL => "solusdt",
            Self::BNB => "bnbusdt",
            Self::AAVE => "aaveusdt",
        }
    }

    // COINM uses <asset>USD_PERP
    fn coinm_stream(self) -> &'static str {
        match self {
            Self::BTC => "btcusd_perp",
            Self::ETH => "ethusd_perp",
            Self::SOL => "solusd_perp",
            Self::BNB => "bnbusd_perp",
            Self::AAVE => "aaveusd_perp",
        }
    }

    // Coinbase product_id for trading pairs
    fn coinbase_product_id(self) -> &'static str {
        match self {
            Self::BTC => "BTC-USD",
            Self::ETH => "ETH-USD",
            _ => "BTC-USD", // Only BTC and ETH for Coinbase
        }
    }
}

// Coinbase "match" message schema
#[derive(Debug, Deserialize)]
struct CoinbaseMatch {
    #[serde(rename = "type")]
    msg_type: String, // "match"
    time: String, // ISO8601
    product_id: String,
    size: String,
    price: String,
    side: String, // maker side: "buy" or "sell"
}

// Coinbase subscribe request
#[derive(Debug, Serialize)]
struct CoinbaseSubscribe<'a> {
    #[serde(rename = "type")]
    msg_type: &'a str, // "subscribe"
    product_ids: Vec<&'a str>,
    channels: Vec<&'a str>, // ["matches"]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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
    fn from_str(s: &str) -> Self {
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

// ws URL builders
fn get_ws_url(exchange: Exchange, market: Market, symbol: Symbol) -> String {
    match exchange {
        Exchange::Binance => binance_ws_url(market, symbol),
        Exchange::Coinbase => "wss://ws-feed.exchange.coinbase.com".to_string(),
    }
}

fn binance_ws_url(market: Market, symbol: Symbol) -> String {
    let sym = match market {
        Market::Coinm => symbol.coinm_stream(),
        _ => symbol.spot_usdm_stream(),
    };

    match market {
        Market::Spot => format!("wss://stream.binance.com:9443/ws/{sym}@aggTrade"),
        Market::Usdm => format!("wss://fstream.binance.com/ws/{sym}@aggTrade"),
        Market::Coinm => format!("wss://dstream.binance.com/ws/{sym}@aggTrade"),
    }
}

// ==============================
// Models
// ==============================

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
    time: i64, // unix seconds (lightweight-charts)
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

// ==============================
// Aggregators
// ==============================

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
    cur_sec: Option<i64>,
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

// ==============================
// State (per market+symbol shard)
// ==============================

#[derive(Clone)]
struct Shard {
    hist_1m: Arc<RwLock<VecDeque<Bar>>>,
    hist_5m: Arc<RwLock<VecDeque<Bar>>>,
    tx_1m: broadcast::Sender<Bar>,
    tx_5m: broadcast::Sender<Bar>,
    cvd_closed_usdt: Arc<RwLock<f64>>,
}

fn mk_shard() -> Shard {
    let (tx_1m, _) = broadcast::channel::<Bar>(2048);
    let (tx_5m, _) = broadcast::channel::<Bar>(2048);
    Shard {
        hist_1m: Arc::new(RwLock::new(VecDeque::with_capacity(4000))),
        hist_5m: Arc::new(RwLock::new(VecDeque::with_capacity(4000))),
        tx_1m,
        tx_5m,
        cvd_closed_usdt: Arc::new(RwLock::new(0.0)),
    }
}

#[derive(Clone)]
struct AppState {
    shards: Arc<HashMap<(Exchange, Market, Symbol), Shard>>,
    db: SqlitePool,
}

fn pick_shard(state: &AppState, exchange: Exchange, market: Market, symbol: Symbol) -> &Shard {
    state.shards.get(&(exchange, market, symbol)).expect("shard missing")
}

// ==============================
// HTTP params
// ==============================

#[derive(Deserialize)]
struct Params {
    exchange: Option<String>,
    market: Option<String>,
    symbol: Option<String>,
    tf: Option<String>,
    limit: Option<usize>,
    before: Option<i64>,
}

// ==============================
// DB + migration
// ==============================

async fn init_db(db: &SqlitePool) -> Result<()> {
    sqlx::query("PRAGMA journal_mode=WAL;").execute(db).await?;
    sqlx::query("PRAGMA synchronous=NORMAL;")
        .execute(db)
        .await?;

    // Detect existing schema
    let cols: Vec<(i64, String, String, i64, Option<String>, i64)> =
        sqlx::query_as(r#"PRAGMA table_info(bars);"#)
            .fetch_all(db)
            .await
            .unwrap_or_default();

    let has_bars = !cols.is_empty();
    let has_symbol_col = cols.iter().any(|c| c.1 == "symbol");

    if has_bars && !has_symbol_col {
        // Old schema: (market, tf, time, ...)
        // Migrate to new schema with symbol column.
        eprintln!("DB migrate: old bars table detected (no symbol). Migrating to bars_v2...");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS bars_v2 (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                tf TEXT NOT NULL,
                time INTEGER NOT NULL,
                price_close REAL NOT NULL,
                delta_usdt REAL NOT NULL,
                cvd_usdt REAL NOT NULL,
                trades INTEGER NOT NULL,
                PRIMARY KEY (market, symbol, tf, time)
            );
            "#,
        )
        .execute(db)
        .await?;

        // Copy old rows, default symbol=BTC
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO bars_v2 (market, symbol, tf, time, price_close, delta_usdt, cvd_usdt, trades)
            SELECT market, 'BTC', tf, time, price_close, delta_usdt, cvd_usdt, trades
            FROM bars;
            "#,
        )
        .execute(db)
        .await?;

        // Replace old table
        sqlx::query("DROP TABLE bars;").execute(db).await?;
        sqlx::query("ALTER TABLE bars_v2 RENAME TO bars;")
            .execute(db)
            .await?;
    }

    // Ensure latest schema exists
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bars (
            exchange TEXT NOT NULL,
            market TEXT NOT NULL,
            symbol TEXT NOT NULL,
            tf TEXT NOT NULL,
            time INTEGER NOT NULL,
            price_close REAL NOT NULL,
            delta_usdt REAL NOT NULL,
            cvd_usdt REAL NOT NULL,
            trades INTEGER NOT NULL,
            PRIMARY KEY (exchange, market, symbol, tf, time)
        );
        "#,
    )
    .execute(db)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_bars_query
        ON bars(exchange, market, symbol, tf, time DESC);
        "#,
    )
    .execute(db)
    .await?;

    Ok(())
}

async fn load_history(
    db: &SqlitePool,
    exchange: &str,
    market: &str,
    symbol: &str,
    tf: &str,
    limit: usize,
) -> Result<(VecDeque<Bar>, f64)> {
    let rows: Vec<BarRow> = sqlx::query_as(
        r#"
        SELECT time, price_close, delta_usdt, cvd_usdt, trades
        FROM bars
        WHERE exchange = ? AND market = ? AND symbol = ? AND tf = ?
        ORDER BY time DESC
        LIMIT ?
        "#,
    )
    .bind(exchange)
    .bind(market)
    .bind(symbol)
    .bind(tf)
    .bind(limit as i64)
    .fetch_all(db)
    .await?;

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

async fn load_history_page(
    db: &SqlitePool,
    exchange: &str,
    symbol: &str,
    market: &str,
    tf: &str,
    limit: usize,
    before: Option<i64>,
) -> Result<Vec<Bar>> {
    let limit = limit.min(4000).max(1);

    let rows: Vec<BarRow> = if let Some(before_ts) = before {
        sqlx::query_as(
            r#"
            SELECT time, price_close, delta_usdt, cvd_usdt, trades
            FROM bars
            WHERE exchange = ? AND symbol = ? AND market = ? AND tf = ?
              AND time < ?
            ORDER BY time DESC
            LIMIT ?
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(market)
        .bind(tf)
        .bind(before_ts)
        .bind(limit as i64)
        .fetch_all(db)
        .await?
    } else {
        sqlx::query_as(
            r#"
            SELECT time, price_close, delta_usdt, cvd_usdt, trades
            FROM bars
            WHERE exchange = ? AND symbol = ? AND market = ? AND tf = ?
            ORDER BY time DESC
            LIMIT ?
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(market)
        .bind(tf)
        .bind(limit as i64)
        .fetch_all(db)
        .await?
    };

    // rows DESC -> 反转成 ASC，前端 setData 方便
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
    Ok(v)
}

async fn hydrate_shard(state: &AppState, exchange: Exchange, market: Market, symbol: Symbol) -> Result<()> {
    let e = exchange.as_str();
    let m = market.as_str();
    let s = symbol.as_str();
    let (h1, last1) = load_history(&state.db, e, m, s, "1m", 4000).await?;
    let (h5, _last5) = load_history(&state.db, e, m, s, "5m", 4000).await?;

    let sh = pick_shard(state, exchange, market, symbol);
    *sh.hist_1m.write().await = h1;
    *sh.hist_5m.write().await = h5;
    *sh.cvd_closed_usdt.write().await = last1;

    Ok(())
}

// ==============================
// Main
// ==============================

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

    // build shards for all exchange × market × symbol
    let mut shards: HashMap<(Exchange, Market, Symbol), Shard> = HashMap::new();
    let exchanges = [Exchange::Binance, Exchange::Coinbase];
    let markets = [Market::Spot, Market::Usdm, Market::Coinm];
    let symbols = [
        Symbol::BTC,
        Symbol::ETH,
        Symbol::SOL,
        Symbol::BNB,
        Symbol::AAVE,
    ];

    for &ex in &exchanges {
        for &m in &markets {
            // Coinbase only has Spot market
            if ex == Exchange::Coinbase && m != Market::Spot {
                continue;
            }
            // Coinbase only supports BTC and ETH
            let syms = if ex == Exchange::Coinbase {
                &[Symbol::BTC, Symbol::ETH]
            } else {
                &symbols[..]
            };
            for &s in syms {
                shards.insert((ex, m, s), mk_shard());
            }
        }
    }

    let state = AppState {
        shards: Arc::new(shards),
        db,
    };

    // hydrate histories for each shard
    for &ex in &exchanges {
        for &m in &markets {
            if ex == Exchange::Coinbase && m != Market::Spot {
                continue;
            }
            let syms = if ex == Exchange::Coinbase {
                &[Symbol::BTC, Symbol::ETH]
            } else {
                &symbols[..]
            };
            for &s in syms {
                hydrate_shard(&state, ex, m, s).await?;
            }
        }
    }

    // start engines concurrently for each exchange × market × symbol
    for &ex in &exchanges {
        for &m in &markets {
            if ex == Exchange::Coinbase && m != Market::Spot {
                continue;
            }
            let syms = if ex == Exchange::Coinbase {
                &[Symbol::BTC, Symbol::ETH]
            } else {
                &symbols[..]
            };
            for &s in syms {
                tokio::spawn(engine_loop(state.clone(), ex, m, s));
            }
        }
    }

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

// ==============================
// HTTP handlers
// ==============================

async fn history(State(state): State<AppState>, Query(p): Query<Params>) -> impl IntoResponse {
    let exchange = p.exchange.as_deref().unwrap_or("binance");
    let symbol = p.symbol.as_deref().unwrap_or("BTC");
    let market = p.market.as_deref().unwrap_or("spot");
    let tf = p.tf.as_deref().unwrap_or("1m");
    let limit = p.limit.unwrap_or(800).min(4000);
    let before = p.before;

    match load_history_page(&state.db, exchange, symbol, market, tf, limit, before).await {
        Ok(bars) => Json(bars).into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("history error: {e}"),
        )
            .into_response(),
    }
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(p): Query<Params>,
) -> impl IntoResponse {
    let exchange = Exchange::from_str(p.exchange.as_deref().unwrap_or("binance"));
    let market = Market::from_str(p.market.as_deref().unwrap_or("spot"));
    let symbol = Symbol::from_str(p.symbol.as_deref().unwrap_or("BTC"));
    let tf = Tf::from_str(p.tf.as_deref().unwrap_or("1m"));
    ws.on_upgrade(move |socket| ws_handler(socket, state, exchange, market, symbol, tf))
}

async fn ws_handler(
    mut socket: WebSocket,
    state: AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    tf: Tf,
) {
    let sh = pick_shard(&state, exchange, market, symbol).clone();

    let mut rx = match tf {
        Tf::M1 => sh.tx_1m.subscribe(),
        Tf::M5 => sh.tx_5m.subscribe(),
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

// ==============================
// Engine
// ==============================

async fn engine_loop(state: AppState, exchange: Exchange, market: Market, symbol: Symbol) {
    let mut attempt: u32 = 0;

    loop {
        let res = run_engine_once(&state, exchange, market, symbol).await;

        match res {
            Ok(_) => attempt = 0,
            Err(e) => {
                let s = e.to_string();
                if s.contains("ws ended") {
                    attempt = 0;
                } else {
                    eprintln!(
                        "[{}:{}:{}] engine error: {e}",
                        exchange.as_str(),
                        market.as_str(),
                        symbol.as_str()
                    );
                    attempt = attempt.saturating_add(1);
                }
            }
        }

        let wait = backoff_with_jitter(attempt);
        eprintln!(
            "[{}:{}:{}] reconnecting in {:.3}s (attempt={})",
            exchange.as_str(),
            market.as_str(),
            symbol.as_str(),
            wait.as_secs_f64(),
            attempt
        );
        sleep(wait).await;
    }
}

async fn run_engine_once(state: &AppState, exchange: Exchange, market: Market, symbol: Symbol) -> Result<()> {
    let url = get_ws_url(exchange, market, symbol);
    eprintln!(
        "[{}:{}:{}] Connecting to: {}",
        exchange.as_str(),
        market.as_str(),
        symbol.as_str(),
        url
    );

    let (ws_stream, _resp) = connect_async(&url).await?;
    let (mut write, mut read) = ws_stream.split();

    let sh = pick_shard(state, exchange, market, symbol);

    match exchange {
        Exchange::Binance => run_binance_engine(state, exchange, market, symbol, &sh, write, read).await,
        Exchange::Coinbase => run_coinbase_engine(state, exchange, market, symbol, &sh, write, read).await,
    }
}

// Helper to parse ISO8601 timestamps to milliseconds
fn parse_iso8601_to_ms(s: &str) -> Result<i64> {
    let dt: DateTime<Utc> = s.parse()?;
    Ok(dt.timestamp_millis())
}

async fn run_binance_engine(
    state: &AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    sh: &Shard,
    mut write: futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, tokio_tungstenite::tungstenite::Message>,
    mut read: futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
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

                        flush_second(
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
            tokio_tungstenite::tungstenite::Message::Close(_) => break,
            _ => {}
        }
    }

    // flush last partial second (best-effort)
    if let Some(s) = sec.cur_sec {
        flush_second(
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

async fn run_coinbase_engine(
    state: &AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    sh: &Shard,
    mut write: futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, tokio_tungstenite::tungstenite::Message>,
    mut read: futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
) -> Result<()> {
    // Subscribe to matches for BTC-USD + ETH-USD
    let product_ids = match symbol {
        Symbol::BTC => vec!["BTC-USD"],
        Symbol::ETH => vec!["ETH-USD"],
        _ => return Err(anyhow!("Coinbase only supports BTC and ETH")),
    };

    let sub = CoinbaseSubscribe {
        msg_type: "subscribe",
        product_ids,
        channels: vec!["matches"],
    };
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&sub)?,
        ))
        .await?;

    let mut cvd_closed_usdt = *sh.cvd_closed_usdt.read().await;
    let mut sec = SecAgg::new();
    let mut a1 = BucketAgg::new();
    let mut a5 = BucketAgg::new();

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            _ = ping.tick() => {
                // Keep-alive ping
                let _ = write.send(
                    tokio_tungstenite::tungstenite::Message::Ping(vec![])
                ).await;
            }

            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                };

                match msg {
                    tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                        write
                            .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                            .await?;
                    }
                    tokio_tungstenite::tungstenite::Message::Text(txt) => {
                        let m: CoinbaseMatch = match serde_json::from_str(&txt) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if m.msg_type != "match" {
                            continue;
                        }

                        let expected_product = match symbol {
                            Symbol::BTC => "BTC-USD",
                            Symbol::ETH => "ETH-USD",
                            _ => unreachable!(),
                        };
                        if m.product_id != expected_product {
                            continue;
                        }

                        let price: f64 = match m.price.parse() {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let size: f64 = match m.size.parse() {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        // side: "buy" (taker buy) => positive, "sell" (taker sell) => negative
                        let signed_size = if m.side == "sell" { size } else { -size };
                        let delta_usdt = signed_size * price;

                        let ts_ms = parse_iso8601_to_ms(&m.time)?;
                        let this_sec = ts_ms / 1000;

                        match sec.cur_sec {
                            None => sec.reset_to(this_sec, price, delta_usdt),
                            Some(s) if s != this_sec => {
                                let sec_ended = s;
                                let sec_last_price = sec.last_price;
                                let sec_delta = sec.delta_usdt;
                                let sec_trades = sec.trades;

                                flush_second(
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
                    tokio_tungstenite::tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }

    // flush last partial second (best-effort)
    if let Some(s) = sec.cur_sec {
        flush_second(
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

async fn flush_second(
    state: &AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    a1: &mut BucketAgg,
    a5: &mut BucketAgg,
    cvd_closed_usdt: &mut f64,
    sec_ended: i64,
    sec_last_price: f64,
    sec_delta_usdt: f64,
    _sec_trades: u64,
) {
    let ts_ms_flush = sec_ended * 1000 + 999;

    // 1m close-only
    if let Some(mut closed_bar_1m) =
        push_bucket(a1, Tf::M1, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        *cvd_closed_usdt += closed_bar_1m.delta_usdt;
        closed_bar_1m.cvd_usdt = *cvd_closed_usdt;
        push_publish(state, exchange, market, symbol, Tf::M1, closed_bar_1m).await;
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
        trades: a1.trades,
    };

    let sh = pick_shard(state, exchange, market, symbol);
    let _ = sh.tx_1m.send(live_bar_1m);

    // 5m close-only
    if let Some(mut closed_bar_5m) =
        push_bucket(a5, Tf::M5, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        closed_bar_5m.cvd_usdt = *cvd_closed_usdt;
        push_publish(state, exchange, market, symbol, Tf::M5, closed_bar_5m).await;
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
            if agg.trades == 0 || agg.last_price.is_nan() {
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
                cvd_usdt: 0.0,
                trades: agg.trades,
            };

            agg.cur_bucket_id = Some(bucket_id);
            agg.last_price = price;
            agg.delta_usdt = delta_usdt;
            agg.trades = 1;

            return Some(bar);
        }
        _ => {}
    }

    agg.last_price = price;
    agg.delta_usdt += delta_usdt;
    agg.trades += 1;

    None
}

async fn push_publish(state: &AppState, exchange: Exchange, market: Market, symbol: Symbol, tf: Tf, bar: Bar) {
    let exchange_str = exchange.as_str();
    let tf_str = tf.as_str();
    let market_str = market.as_str();
    let symbol_str = symbol.as_str();

    // 1) persist (UPSERT)
    if let Err(e) = sqlx::query(
        r#"
        INSERT INTO bars (exchange, market, symbol, tf, time, price_close, delta_usdt, cvd_usdt, trades)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(exchange, market, symbol, tf, time) DO UPDATE SET
            price_close=excluded.price_close,
            delta_usdt=excluded.delta_usdt,
            cvd_usdt=excluded.cvd_usdt,
            trades=excluded.trades
        "#,
    )
    .bind(exchange_str)
    .bind(market_str)
    .bind(symbol_str)
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
            "db write failed exchange={} market={} symbol={} tf={} time={} err={e}",
            exchange_str, market_str, symbol_str, tf_str, bar.time
        );
    }

    // 2) in-memory history + ws broadcast for CLOSE bars
    let sh = pick_shard(state, exchange, market, symbol);
    let max = 4000usize;

    match tf {
        Tf::M1 => {
            let mut h = sh.hist_1m.write().await;
            if h.len() >= max {
                h.pop_front();
            }
            h.push_back(bar.clone());
            let _ = sh.tx_1m.send(bar);
        }
        Tf::M5 => {
            let mut h = sh.hist_5m.write().await;
            if h.len() >= max {
                h.pop_front();
            }
            h.push_back(bar.clone());
            let _ = sh.tx_5m.send(bar);
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

mod types;
mod models;
mod aggregator;
mod binance;
mod coinbase;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{Query, State, WebSocketUpgrade},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use rand::Rng;
use serde::Deserialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::str::FromStr;
use std::{collections::VecDeque, net::SocketAddr, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tower_http::services::ServeDir;

use types::{Exchange, Market, Symbol, Tf};
use models::Bar;

// ==============================
// State / Shard
// ==============================

#[derive(Clone)]
pub struct Shard {
    pub hist_1m: Arc<RwLock<VecDeque<Bar>>>,
    pub hist_5m: Arc<RwLock<VecDeque<Bar>>>,
    pub tx_1m: broadcast::Sender<Bar>,
    pub tx_5m: broadcast::Sender<Bar>,
    pub cvd_closed_usdt: Arc<RwLock<f64>>,
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

pub struct AppState {
    pub shards: Arc<HashMap<(Exchange, Market, Symbol), Shard>>,
    pub db: SqlitePool,
}

fn pick_shard(state: &AppState, exchange: Exchange, market: Market, symbol: Symbol) -> &Shard {
    state.shards.get(&(exchange, market, symbol)).expect("shard missing")
}

// Helper: Get valid (market, symbol) pairs for a given exchange
fn get_market_symbol_pairs(exchange: Exchange) -> Vec<(Market, Symbol)> {
    match exchange {
        Exchange::Coinbase => vec![(Market::Spot, Symbol::Btc), (Market::Spot, Symbol::Eth)],
        Exchange::Binance => {
            let markets = [Market::Spot, Market::Usdm, Market::Coinm];
            let symbols = [Symbol::Btc, Symbol::Eth, Symbol::Sol, Symbol::Bnb, Symbol::Aave];
            markets.iter().flat_map(|&m| symbols.iter().map(move |&s| (m, s))).collect()
        }
    }
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
    let has_exchange_col = cols.iter().any(|c| c.1 == "exchange");

    if has_bars && !has_exchange_col {
        eprintln!("DB migrate: old bars table detected (no exchange). Migrating to bars_v2...");

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS bars_v2 (
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
            INSERT OR IGNORE INTO bars_v2 (exchange, market, symbol, tf, time, price_close, delta_usdt, cvd_usdt, trades)
            SELECT 'binance', market, symbol, tf, time, price_close, delta_usdt, cvd_usdt, trades
            FROM bars;
            "#,
        )
        .execute(db)
        .await?;

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
    let rows: Vec<models::BarRow> = sqlx::query_as(
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

    let (v, last_cvd) = convert_rows_to_bars(rows);
    Ok((VecDeque::from(v), last_cvd))
}

fn convert_rows_to_bars(rows: Vec<models::BarRow>) -> (Vec<Bar>, f64) {
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
    (v, last_cvd)
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
    let limit = limit.clamp(1, 4000);

    let rows: Vec<models::BarRow> = if let Some(before_ts) = before {
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

    let (v, _) = convert_rows_to_bars(rows);
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

    for &ex in &exchanges {
        for (m, s) in get_market_symbol_pairs(ex) {
            shards.insert((ex, m, s), mk_shard());
        }
    }

    let state = AppState {
        shards: Arc::new(shards),
        db,
    };
    let state = Arc::new(state);

    // hydrate histories for each shard
    for &ex in &exchanges {
        for (m, s) in get_market_symbol_pairs(ex) {
            hydrate_shard(&state, ex, m, s).await?;
        }
    }

    // start engines concurrently for each exchange × market × symbol
    for &ex in &exchanges {
        for (m, s) in get_market_symbol_pairs(ex) {
            let state_clone = state.clone();
            tokio::spawn(engine_loop(state_clone, ex, m, s));
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

async fn history(State(state): State<Arc<AppState>>, Query(p): Query<Params>) -> impl IntoResponse {
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
    State(state): State<Arc<AppState>>,
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
    state: Arc<AppState>,
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

async fn engine_loop(state: Arc<AppState>, exchange: Exchange, market: Market, symbol: Symbol) {
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

pub fn binance_ws_url(market: Market, symbol: Symbol) -> String {
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

pub fn get_ws_url(exchange: Exchange, market: Market, symbol: Symbol) -> String {
    match exchange {
        Exchange::Binance => binance_ws_url(market, symbol),
        Exchange::Coinbase => "wss://ws-feed.exchange.coinbase.com".to_string(),
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
    let (write, read) = ws_stream.split();

    let sh = pick_shard(state, exchange, market, symbol);

    match exchange {
        Exchange::Binance => binance::run_binance_engine(state, exchange, market, symbol, sh, write, read).await,
        Exchange::Coinbase => coinbase::run_coinbase_engine(state, exchange, market, symbol, sh, write, read).await,
    }
}

pub async fn flush_second(
    state: &AppState,
    exchange: Exchange,
    market: Market,
    symbol: Symbol,
    a1: &mut aggregator::BucketAgg,
    a5: &mut aggregator::BucketAgg,
    cvd_closed_usdt: &mut f64,
    sec_ended: i64,
    sec_last_price: f64,
    sec_delta_usdt: f64,
    _sec_trades: u64,
) {
    let ts_ms_flush = sec_ended * 1000 + 999;

    // 1m close-only
    if let Some(mut closed_bar_1m) =
        aggregator::push_bucket(a1, Tf::M1, ts_ms_flush, sec_last_price, sec_delta_usdt)
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
        aggregator::push_bucket(a5, Tf::M5, ts_ms_flush, sec_last_price, sec_delta_usdt)
    {
        closed_bar_5m.cvd_usdt = *cvd_closed_usdt;
        push_publish(state, exchange, market, symbol, Tf::M5, closed_bar_5m).await;
    }

    // 5m live every second (update last point)
    let tf_ms = Tf::M5.ms();
    let cur_bucket_id = ts_ms_flush / tf_ms;
    let cur_end_ms = (cur_bucket_id + 1) * tf_ms - 1;

    let live_bar_5m = Bar {
        time: cur_end_ms / 1000,
        price_close: a5.last_price,
        delta_usdt: a5.delta_usdt,
        cvd_usdt: *cvd_closed_usdt + a5.delta_usdt,
        trades: a5.trades,
    };

    let _ = sh.tx_5m.send(live_bar_5m);
}

async fn push_publish(state: &AppState, exchange: Exchange, market: Market, symbol: Symbol, tf: Tf, bar: Bar) {
    let exchange_str = exchange.as_str();
    let tf_str = tf.as_str();
    let market_str = market.as_str();
    let symbol_str = symbol.as_str();

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

pub fn parse_iso8601_to_ms(s: &str) -> Result<i64> {
    let dt: DateTime<Utc> = s.parse()?;
    Ok(dt.timestamp_millis())
}

fn backoff_with_jitter(attempt: u32) -> Duration {
    let exp = (1u64 << attempt.min(6)).min(60);
    let base = exp.max(1);
    let jitter_ms: u64 = rand::thread_rng().gen_range(0..=250);
    Duration::from_secs(base) + Duration::from_millis(jitter_ms)
}

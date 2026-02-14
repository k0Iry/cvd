use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct AggTrade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

// Coinbase "match" message schema
#[derive(Debug, Deserialize)]
pub struct CoinbaseMatch {
    #[serde(rename = "type")]
    pub msg_type: String, // "match"
    pub time: String, // ISO8601
    pub product_id: String,
    pub size: String,
    pub price: String,
    pub side: String, // maker side: "buy" or "sell"
}

// Coinbase subscribe request
#[derive(Debug, Serialize)]
pub struct CoinbaseSubscribe<'a> {
    #[serde(rename = "type")]
    pub msg_type: &'a str, // "subscribe"
    pub product_ids: Vec<&'a str>,
    pub channels: Vec<&'a str>, // ["matches"]
}

#[derive(Debug, Clone, Serialize)]
pub struct Bar {
    pub time: i64, // unix seconds (lightweight-charts)
    pub price_close: f64,
    pub delta_usdt: f64,
    pub cvd_usdt: f64,
    pub trades: u64,
}

#[derive(sqlx::FromRow)]
pub struct BarRow {
    pub time: i64,
    pub price_close: f64,
    pub delta_usdt: f64,
    pub cvd_usdt: f64,
    pub trades: i64,
}

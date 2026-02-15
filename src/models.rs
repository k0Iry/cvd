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

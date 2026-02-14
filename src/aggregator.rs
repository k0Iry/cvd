use crate::models::Bar;
use crate::types::Tf;

#[derive(Debug, Clone)]
pub struct BucketAgg {
    pub cur_bucket_id: Option<i64>,
    pub last_price: f64,
    pub delta_usdt: f64,
    pub trades: u64,
}
impl BucketAgg {
    pub fn new() -> Self {
        Self {
            cur_bucket_id: None,
            last_price: f64::NAN,
            delta_usdt: 0.0,
            trades: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SecAgg {
    pub cur_sec: Option<i64>,
    pub last_price: f64,
    pub delta_usdt: f64,
    pub trades: u64,
}
impl SecAgg {
    pub fn new() -> Self {
        Self {
            cur_sec: None,
            last_price: f64::NAN,
            delta_usdt: 0.0,
            trades: 0,
        }
    }

    pub fn reset_to(&mut self, sec: i64, price: f64, delta_usdt: f64) {
        self.cur_sec = Some(sec);
        self.last_price = price;
        self.delta_usdt = delta_usdt;
        self.trades = 1;
    }

    pub fn add(&mut self, price: f64, delta_usdt: f64) {
        self.last_price = price;
        self.delta_usdt += delta_usdt;
        self.trades += 1;
    }
}

pub fn push_bucket(
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

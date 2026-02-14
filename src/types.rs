// ==============================
// Types: Exchange / Market / Symbol / Tf
// ==============================

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Exchange {
    Binance,
    Coinbase,
}
impl Exchange {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "coinbase" => Self::Coinbase,
            _ => Self::Binance,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Coinbase => "coinbase",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Market {
    Spot,
    Usdm,
    Coinm,
}
impl Market {
    pub fn from_str(s: &str) -> Self {
        match s {
            "usdm" => Self::Usdm,
            "coinm" => Self::Coinm,
            _ => Self::Spot,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Usdm => "usdm",
            Self::Coinm => "coinm",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Symbol {
    BTC,
    ETH,
    SOL,
    BNB,
    AAVE,
}
impl Symbol {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ETH" => Self::ETH,
            "SOL" => Self::SOL,
            "BNB" => Self::BNB,
            "AAVE" => Self::AAVE,
            _ => Self::BTC,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BTC => "BTC",
            Self::ETH => "ETH",
            Self::SOL => "SOL",
            Self::BNB => "BNB",
            Self::AAVE => "AAVE",
        }
    }

    // Spot / USDM use <asset>USDT
    pub fn spot_usdm_stream(self) -> &'static str {
        match self {
            Self::BTC => "btcusdt",
            Self::ETH => "ethusdt",
            Self::SOL => "solusdt",
            Self::BNB => "bnbusdt",
            Self::AAVE => "aaveusdt",
        }
    }

    // COINM uses <asset>USD_PERP
    pub fn coinm_stream(self) -> &'static str {
        match self {
            Self::BTC => "btcusd_perp",
            Self::ETH => "ethusd_perp",
            Self::SOL => "solusd_perp",
            Self::BNB => "bnbusd_perp",
            Self::AAVE => "aaveusd_perp",
        }
    }

    // Coinbase product_id for trading pairs
    pub fn coinbase_product_id(self) -> &'static str {
        match self {
            Self::BTC => "BTC-USD",
            Self::ETH => "ETH-USD",
            _ => "BTC-USD", // Only BTC and ETH for Coinbase
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Tf {
    M1,
    M5,
}
impl Tf {
    pub fn ms(self) -> i64 {
        match self {
            Tf::M1 => 60_000,
            Tf::M5 => 300_000,
        }
    }
    pub fn from_str(s: &str) -> Self {
        match s {
            "5m" => Tf::M5,
            _ => Tf::M1,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Tf::M1 => "1m",
            Tf::M5 => "5m",
        }
    }
}

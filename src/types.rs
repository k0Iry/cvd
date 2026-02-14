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
    Btc,
    Eth,
    Sol,
    Bnb,
    Aave,
}
impl Symbol {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ETH" => Self::Eth,
            "SOL" => Self::Sol,
            "BNB" => Self::Bnb,
            "AAVE" => Self::Aave,
            _ => Self::Btc,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
            Self::Sol => "SOL",
            Self::Bnb => "BNB",
            Self::Aave => "AAVE",
        }
    }

    // Spot / USDM use <asset>USDT
    pub fn spot_usdm_stream(self) -> &'static str {
        match self {
            Self::Btc => "btcusdt",
            Self::Eth => "ethusdt",
            Self::Sol => "solusdt",
            Self::Bnb => "bnbusdt",
            Self::Aave => "aaveusdt",
        }
    }

    // COINM uses <asset>USD_PERP
    pub fn coinm_stream(self) -> &'static str {
        match self {
            Self::Btc => "btcusd_perp",
            Self::Eth => "ethusd_perp",
            Self::Sol => "solusd_perp",
            Self::Bnb => "bnbusd_perp",
            Self::Aave => "aaveusd_perp",
        }
    }

    // Coinbase product_id for trading pairs
    pub fn coinbase_product_id(self) -> &'static str {
        match self {
            Self::Btc => "BTC-USD",
            Self::Eth => "ETH-USD",
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

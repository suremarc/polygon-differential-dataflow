use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::core::{Decimal, Trade};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EventType {
    XT,
    T,
    #[serde(rename = "status")]
    Status,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Connected,
    AuthSuccess,
    Success,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StatusUpdate {
    pub ev: EventType,
    pub status: Status,
    pub message: arrayvec::ArrayString<32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    Auth,
    Subscribe,
}

#[derive(
    Debug, Clone, Copy, Serialize_repr, Deserialize_repr, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[repr(u8)]
pub enum Tape {
    Missing = 0,
    A = 1,
    B = 2,
    C = 3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub action: ActionType,
    pub params: String,
}

pub trait WebsocketTrade: Trade {
    const SOCKET_PATH: &'static str;
    const FEED_PREFIX: &'static str;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StockTrade {
    pub t: u64,
    pub sym: arrayvec::ArrayString<8>,
    pub x: u32,
    pub z: Tape,
    pub p: Decimal,
    pub s: u32,
    #[serde(default = "default_c")]
    pub c: tinyvec::ArrayVec<[u32; 6]>,
}

fn default_c() -> tinyvec::ArrayVec<[u32; 6]> {
    tinyvec::array_vec!([u32; 6])
}

impl Trade for StockTrade {
    fn ticker(&self) -> &str {
        self.sym.as_str()
    }
    fn price(&self) -> Decimal {
        self.p
    }
    fn volume(&self) -> Decimal {
        Decimal(rust_decimal::Decimal::new(self.s as i64, 0))
    }
    fn timestamp(&self) -> Duration {
        Duration::from_millis(self.t)
    }
    fn exchange(&self) -> u32 {
        self.x
    }
}

impl WebsocketTrade for StockTrade {
    const SOCKET_PATH: &'static str = "stocks";
    const FEED_PREFIX: &'static str = "T";
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CryptoTrade {
    pub t: u64,
    pub pair: arrayvec::ArrayString<12>,
    pub p: Decimal,
    pub s: Decimal,
    pub c: tinyvec::ArrayVec<[u32; 4]>,
    pub x: u32,
    pub r: u64,
}

impl Trade for CryptoTrade {
    fn ticker(&self) -> &str {
        self.pair.as_str()
    }
    fn price(&self) -> Decimal {
        self.p
    }
    fn volume(&self) -> Decimal {
        self.s
    }
    fn timestamp(&self) -> Duration {
        Duration::from_millis(self.t)
    }
    fn exchange(&self) -> u32 {
        self.x
    }
}

impl WebsocketTrade for CryptoTrade {
    const SOCKET_PATH: &'static str = "crypto";
    const FEED_PREFIX: &'static str = "XT";
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message<T: Trade> {
    #[serde(deserialize_with = "T::deserialize")]
    Trade(T),
    Status(StatusUpdate),
}

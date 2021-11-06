use abomonation::Abomonation;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use rust_decimal::Decimal;

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

#[derive(Debug, Clone, Copy, Serialize_repr, Deserialize_repr, PartialEq, Eq, PartialOrd, Ord)]
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

pub trait Trade: Copy + Serialize + serde::de::DeserializeOwned {
    fn ticker(&self) -> String;
    fn price(&self) -> Decimal;
    fn timestamp(&self) -> u64;
    fn exchange(&self) -> u32;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
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

impl Abomonation for StockTrade {}

fn default_c() -> tinyvec::ArrayVec<[u32; 6]> {
    tinyvec::array_vec!([u32; 6])
}

impl Trade for StockTrade {
    fn ticker(&self) -> String {
        String::from(self.sym.as_str())
    }
    fn price(&self) -> Decimal {
        self.p
    }
    fn timestamp(&self) -> u64 {
        self.t
    }
    fn exchange(&self) -> u32 {
        self.x
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CryptoTrade {
    pub t: u64,
    pub pair: arrayvec::ArrayString<10>,
    pub p: Decimal,
    pub s: rust_decimal::Decimal,
    pub c: tinyvec::ArrayVec<[u32; 4]>,
    pub x: u32,
    pub r: u64,
}

impl Abomonation for CryptoTrade {}

impl Trade for CryptoTrade {
    fn ticker(&self) -> String {
        String::from(self.pair.as_str())
    }
    fn price(&self) -> Decimal {
        self.p
    }
    fn timestamp(&self) -> u64 {
        self.t
    }
    fn exchange(&self) -> u32 {
        self.x
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message<T: Trade> {
    #[serde(deserialize_with = "T::deserialize")]
    Trade(T),
    Status(StatusUpdate),
}

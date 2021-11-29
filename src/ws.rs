use std::{
    fmt::{Display, LowerExp},
    iter::Sum,
    ops::{Add, AddAssign, Div, Mul, Neg},
    time::Duration,
};

use differential_dataflow::difference::{Monoid, Semigroup};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal(pub rust_decimal::Decimal);

impl Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl LowerExp for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::LowerExp::fmt(&self.0, f)
    }
}

impl From<isize> for Decimal {
    fn from(x: isize) -> Self {
        Decimal(rust_decimal::Decimal::new(x as i64, 0))
    }
}

impl Add for Decimal {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Decimal(self.0 + rhs.0)
    }
}

impl<'a> AddAssign<&'a Decimal> for Decimal {
    fn add_assign(&mut self, rhs: &'a Self) {
        self.0.add_assign(rhs.0)
    }
}

impl Mul for Decimal {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        Decimal(self.0 * rhs.0)
    }
}

impl Mul<isize> for Decimal {
    type Output = Self;
    fn mul(self, rhs: isize) -> Decimal {
        self * Decimal(rust_decimal::Decimal::new(rhs as i64, 0))
    }
}

impl Mul<Decimal> for isize {
    type Output = Decimal;
    fn mul(self, rhs: Decimal) -> Decimal {
        rhs * Decimal(rust_decimal::Decimal::new(self as i64, 0))
    }
}

impl Div for Decimal {
    type Output = Self;
    fn div(self, rhs: Self) -> Self {
        Decimal(self.0 / rhs.0)
    }
}

impl Div<isize> for Decimal {
    type Output = Self;
    fn div(self, rhs: isize) -> Self {
        Decimal(self.0 / rust_decimal::Decimal::new(rhs as i64, 0))
    }
}

impl Neg for Decimal {
    type Output = Self;
    fn neg(self) -> Self {
        Decimal(-self.0)
    }
}

impl Sum for Decimal {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Decimal(iter.map(|dec| dec.0).sum())
    }
}

impl Semigroup for Decimal {
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl Monoid for Decimal {
    fn zero() -> Self {
        Decimal(rust_decimal::Decimal::ZERO)
    }
}

pub trait Trade: Copy + Serialize + serde::de::DeserializeOwned {
    const SOCKET_PATH: &'static str;
    const FEED_PREFIX: &'static str;

    fn ticker(&self) -> String;
    fn price(&self) -> Decimal;
    fn volume(&self) -> Decimal;
    fn timestamp(&self) -> Duration;
    fn exchange(&self) -> u32;
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
    const SOCKET_PATH: &'static str = "stocks";
    const FEED_PREFIX: &'static str = "T";

    fn ticker(&self) -> String {
        String::from(self.sym.as_str())
    }
    fn price(&self) -> Decimal {
        self.p
    }
    fn volume(&self) -> Decimal {
        Decimal(rust_decimal::Decimal::new(self.s as i64, 0))
    }
    fn timestamp(&self) -> Duration {
        Duration::from_nanos(self.t)
    }
    fn exchange(&self) -> u32 {
        self.x
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CryptoTrade {
    pub t: u64,
    pub pair: arrayvec::ArrayString<10>,
    pub p: Decimal,
    pub s: Decimal,
    pub c: tinyvec::ArrayVec<[u32; 4]>,
    pub x: u32,
    pub r: u64,
}

impl Trade for CryptoTrade {
    const SOCKET_PATH: &'static str = "crypto";
    const FEED_PREFIX: &'static str = "XT";

    fn ticker(&self) -> String {
        String::from(self.pair.as_str())
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message<T: Trade> {
    #[serde(deserialize_with = "T::deserialize")]
    Trade(T),
    Status(StatusUpdate),
}

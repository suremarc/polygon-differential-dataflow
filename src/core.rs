use std::time::Duration;

use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal(pub rust_decimal::Decimal);

pub trait Trade: Serialize + for<'a> Deserialize<'a> + Clone {
    fn ticker(&self) -> &str;

    fn price(&self) -> Decimal;
    fn volume(&self) -> Decimal;
    fn timestamp(&self) -> Duration;
    fn exchange(&self) -> u32;
}

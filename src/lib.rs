#![deny(missing_docs)]
//! 一个简单的用于存储键值对的库。

pub use error::{KvsError, Result};
pub use engines::{KvStore, KvsEngine};

mod error;
mod engines;
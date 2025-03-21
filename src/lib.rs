#![deny(missing_docs)]
//! 一个简单的用于存储键值对的库。

pub use error::{KvsError, Result};
pub use kv::KvStore;
pub use persist::Operation;

mod error;
mod kv;
mod persist;
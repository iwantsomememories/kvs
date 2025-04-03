#![deny(missing_docs)]
//! 一个简单的用于存储键值对的库。

pub use error::{KvsError, Result};
pub use engines::{KvStore, KvsEngine, SledEngine};
pub use client::KvsClient;
pub use server::KvsServer;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

mod error;
mod engines;
mod server;
mod client;
mod common;
pub mod thread_pool;
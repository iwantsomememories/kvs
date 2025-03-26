use failure::Fail;
use std::io;

/// kvs 错误类型.
#[derive(Debug, Fail)]
pub enum KvsError {
    /// IO 错误.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// 序列化与反序列化错误.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// 移除不存在的键.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// 无效命令.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

/// kvs中的Result类型
pub type Result<T> = std::result::Result<T, KvsError>;

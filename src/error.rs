use failure::Fail;
use std::io;
use std::string::FromUtf8Error;

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
    /// sled引擎错误
    #[fail(display = "{}", _0)]
    SledError(#[cause] sled::Error),
    /// 字符串转化错误
    #[fail(display = "{}", _0)]
    Utf8(#[cause] FromUtf8Error),
    /// 附带string信息的错误.
    #[fail(display = "{}", _0)]
    StringError(String),
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

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::SledError(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> Self {
        KvsError::Utf8(err)
    }
}

/// kvs中的Result类型
pub type Result<T> = std::result::Result<T, KvsError>;

//! 该模块包含各个键值对存储引擎

use crate::error::Result;

/// 键值对存储引擎特征
pub trait KvsEngine {
    /// 设置string键值对
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// 根据给定键返回对应值
    /// 
    /// 若键不存在，则返回None
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// 删除给定键
    /// 
    /// # Errors
    /// 
    /// 若给定键不存在，则返回'KvsError::KeyNotFound'
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use kvs::KvStore;
pub use sled::SledEngine;
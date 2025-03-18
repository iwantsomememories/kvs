#![deny(missing_docs)]
//! 一个简单的用于存储键值对的库。

use std::collections::HashMap;

/// 在内存中存储键值对的数据结构，包含一个HashMap。
pub struct KvStore {
    db: HashMap<String, String>,
}

#[allow(unused_variables)]
impl KvStore {

    /// 生成一个KvStore
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// 增加或修改键值对
    pub fn set(&mut self, key: String, value: String) {
        self.db.insert(key, value);
    }

    /// 根据键返回对应值，若不包含该键值对，则返回None
    /// # Example
    /// 
    /// ```
    /// let mut kvs = KvStore::new();
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// println!("{:?}", kvs.get("key1".to_owned()));
    /// println!("{:?}", kvs.get("key2".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.db.get(&key).cloned()
    }

    /// 移除键值对
    pub fn remove(&mut self, key: String) {
        self.db.remove(&key);
    }
}
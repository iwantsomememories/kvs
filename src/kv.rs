use std::collections::HashMap;
use std::path::PathBuf;
use crate::{KvsError, Result, Operation};
use std::fs;
use std::path::Path;
use std::io;

/// 在内存中存储键值对的数据结构，包含一个HashMap。
pub struct KvStore {
    db: HashMap<String, String>,
    /// log文件路径
    path: PathBuf,
}


impl KvStore {
    // /// 生成一个KvStore
    // pub fn new() -> Self {
    //     Self { db: HashMap::new() }
    // }

    /// 增加或修改键值对
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set { key: key.clone(), value: value.clone() };
        self.db.insert(key, value);
        Ok(())
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
    pub fn get(&self, key: String) -> Result<Option<String>> {
        // self.db.get(&key).cloned()
        panic!("unimplemented!")
    }

    /// 移除键值对
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(&key);
        panic!("unimplemented!")
    }

    /// 根据给定路径返回一个KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let db = HashMap::new();
        let path: PathBuf = path.into();

        let ext = path.extension().ok_or(KvsError::UnexpectedCommandType)?;
        
        if ext != ".log" {
            return Err(KvsError::UnexpectedCommandType);
        }
        
        if fs::metadata(path).is_err() {
            fs::File::create(path)?;
        }

        let log_file = fs::File::open(path)?;


        Ok(())
    }
}
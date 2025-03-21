use std::collections::HashMap;
use std::path::PathBuf;
use serde::Deserialize;

use crate::{KvsError, Result, Operation};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Seek};

/// 在内存中存储键值对的数据结构，包含一个HashMap。
pub struct KvStore {
    db: HashMap<String, String>,
    /// log文件路径
    path: PathBuf,
    /// log文件的写入流
    writer: BufWriter<File>,
}


impl KvStore {
    // /// 生成一个KvStore
    // pub fn new() -> Self {
    //     Self { db: HashMap::new() }
    // }

    /// 根据给定路径返回一个KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut db = HashMap::new();
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;

        let log_path = path.join(".kvs.log");
        
        if !log_path.exists() {
            let log_file = File::create(&log_path)?;
            let writer = BufWriter::new(log_file);

            Ok(KvStore { db: db, path: path, writer: writer })
        } else {
            let log_file = File::open(&log_path)?;
            let reader = BufReader::new(log_file);

            let mut deserializer = serde_json::Deserializer::from_reader(reader);

            while let Ok(op) = Operation::deserialize(&mut deserializer) {
                match op {
                    Operation::Set { key, value } => {
                        db.insert(key, value);
                    },
                    Operation::Rm { key } => {
                        db.remove(&key);
                    }
                }
            }

            let log_file = fs::OpenOptions::new().append(true).open(&log_path)?;
            let writer = BufWriter::new(log_file);
            // writer.seek(io::SeekFrom::End(0))?;

            Ok(KvStore { db: db, path: path, writer: writer})
        }
    }


    /// 增加或修改键值对
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set { key: key.clone(), value: value.clone() };
        serde_json::to_writer(&mut self.writer, &op)?;
        self.db.insert(key, value);
        Ok(())
    }

    /// 根据键返回对应值，若不包含该键值对，则返回None
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.db.get(&key).cloned())
    }

    /// 移除键值对
    pub fn remove(&mut self, key: String) -> Result<()> {
        let op = Operation::Rm { key: key.clone() };
        self.db.get(&key).ok_or(KvsError::KeyNotFound)?;
        serde_json::to_writer(&mut self.writer, &op)?;
        self.db.remove(&key);
        Ok(())
    }
}
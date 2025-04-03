use sled::{Db, Tree};
use super::KvsEngine;
use crate::{KvsError, Result};
use std::sync::{Arc, Mutex};


/// sled::Db包装
#[derive(Clone)]
pub struct SledEngine {
    db: Arc<Mutex<Db>>,
}

impl SledEngine {
    /// 根据给定Db生成一个SledEngine
    pub fn new(db: Db) -> Self {
        SledEngine { db: Arc::new(Mutex::new(db)) }
    }
}

impl KvsEngine for SledEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let db = self.db.lock()?;

        let tree: &Tree = &db;
        let res = tree.get(key.as_bytes())?;
        match res {
            None => Ok(None),
            Some(ivec) => {
                let vec = ivec.to_vec();
                let str = String::from_utf8(vec)?;
                Ok(Some(str))
            }
        }
    }  

    fn remove(&self, key: String) -> Result<()> {
        let db = self.db.lock()?;

        let tree: &Tree = &db;
        tree.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let db = self.db.lock()?;

        let tree: &Tree = &db;
        tree.insert(key.as_bytes(), value.as_bytes())?;
        tree.flush()?;
        Ok(())
    }
}


use sled::{Db, Tree};
use super::KvsEngine;
use crate::{KvsError, Result};

/// sled::Db包装
#[derive(Clone)]
pub struct SledEngine {
    db: Db,
}


impl SledEngine {
    /// 根据给定Db生成一个SledEngine
    pub fn new(db: Db) -> Self {
        SledEngine { db }
    }
}

impl KvsEngine for SledEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.db;
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

    fn remove(&mut self, key: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.insert(key.as_bytes(), value.as_bytes())?;
        tree.flush()?;
        Ok(())
    }
}


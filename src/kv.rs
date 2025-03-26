use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use crate::{load, log_path, new_log_file, sorted_gen_list, BufReaderWithPos, BufWriterWithPos, KvsError, Operation, OperationPos, Result};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// 冗余log文件内存大小上限
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// 在内存中存储键值对的数据结构，包含一个HashMap。
pub struct KvStore {
    // 存储log文件的目录路径
    path: PathBuf,
    // log文件编号到文件读取器的映射
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // 当前log文件的写入流
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    index: BTreeMap<String, OperationPos>,
    // 经过压缩可以节约的空间，大小以字节为单位
    uncompacted: u64
}


impl KvStore {
    /// 根据给定路径返回一个KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for &gen in gen_list.iter() {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }
        
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore { 
            path, 
            readers, 
            writer, 
            current_gen, 
            index, 
            uncompacted
        })
    }

    /// 增加或修改键值对
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set { key: key, value: value };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &op)?;
        self.writer.flush()?;
        if let Operation::Set { key, .. } = op {
            if let Some(old_op) = self
                .index
                .insert(key, (self.current_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += old_op.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// 根据键返回对应值，若不包含该键值对，则返回None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(op_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&op_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(op_pos.pos))?;
            let op_reader = reader.take(op_pos.len);
            if let Operation::Set { key:_, value } = serde_json::from_reader(op_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// 移除键值对
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let op = Operation::Rm { key };
            serde_json::to_writer(&mut self.writer, &op)?;
            self.writer.flush()?;
            if let Operation::Rm { key } = op {
                let old_op = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_op.len;
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// 清除无用记录
    pub fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        let mut new_pos = 0;
        // 读取有效记录，写入压缩log文件中
        for op_pos in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&op_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != op_pos.pos {
                reader.seek(SeekFrom::Start(op_pos.pos))?;
            }

            let mut entry_reader = reader.take(op_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *op_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // 删除无效log文件
        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;

        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }
}
use std::collections::{BTreeMap, HashMap};
use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use super::KvsEngine;

/// 冗余log文件内存大小上限
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// 在内存中存储键值对的数据结构
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
    uncompacted: u64,
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
            uncompacted,
        })
    }

    /// 增加或修改键值对
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set { key, value };
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
            if let Operation::Set { key: _, value } = serde_json::from_reader(op_reader)? {
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
    fn compact(&mut self) -> Result<()> {
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

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.set(key, value)
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.get(key)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.remove(key)
    }
}



/// 保存在磁盘上的操作
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
enum Operation {
    /// 设置键值对
    Set {
        /// 键
        key: String,
        /// 值
        value: String,
    },
    /// 删除键
    Rm {
        /// 键
        key: String,
    },
}

/// 记录操作在log文件中的位置及长度
pub struct OperationPos {
    /// 编号
    gen: u64,
    /// 偏移量
    pos: u64,
    /// 长度
    len: u64,
}

impl From<(u64, Range<u64>)> for OperationPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        OperationPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

/// 根据给定编号生成日志文件，并将读取器加入映射，返回该日志的写入器
fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, gen);
    let writer = BufWriterWithPos::new(OpenOptions::new().create(true).append(true).open(&path)?)?;

    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

/// 返回给定目录中log文件的有序编号数组
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// 读取单个log文件，并在index中存入值所在位置。返回压缩后可以节约多少字节
fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, OperationPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Operation>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Operation::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Operation::Rm { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

/// 根据gen返回log文件路径
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// 附带偏移量的BufReader
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    /// 读取器偏移量
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    /// BufReaderWithPos的构造器
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

/// 附带偏移量的BufWriter
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    /// 写入器偏移量
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    /// BufWriterWithPos的构造器
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

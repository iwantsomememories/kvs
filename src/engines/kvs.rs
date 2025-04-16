use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::KvsEngine;
use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crossbeam_skiplist::SkipMap;

/// 冗余log文件内存大小上限
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// KvStore多线程安全共享的实现
#[derive(Clone)]
pub struct KvStore {
    // 存储log文件的目录路径
    path: Arc<PathBuf>,
    // log文件编号到文件读取器的映射
    index: Arc<SkipMap<String, OperationPos>>,
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

impl KvStore {
    /// 根据给定路径返回一个KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let index = Arc::new(SkipMap::new());

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for &gen in gen_list.iter() {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &*index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen)?;
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = KvStoreReader {
            path: Arc::clone(&path),
            safe_point,
            readers: RefCell::new(readers),
        };

        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            current_gen,
            uncompacted,
            path: Arc::clone(&path),
            index: Arc::clone(&index),
        };

        Ok(KvStore {
            path,
            reader,
            index,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

impl KvsEngine for KvStore {
    /// 根据键返回对应值，若不包含该键值对，则返回None
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(op_pos) = self.index.get(&key) {
            if let Operation::Set { key, value } = self.reader.read_operation(*op_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// 移除键值对
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }

    /// 增加或修改键值对
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }
}

/// 单线程读取器
/// 
/// 每一个'KvStore'实例都有自己的'KvStoreReader'，
/// 'KvStoreReader'可以隔离打开相同的文件。
struct KvStoreReader {
    path: Arc<PathBuf>,
    // 最新的压缩文件版本
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

impl KvStoreReader {
    /// 关闭版本号小于安全点（safe_point）的文件句柄。
    ///
    /// 安全点会在压缩完成后更新为最新的压缩版本号。
    /// 压缩版本包含了该操作之前的所有操作总和，且内存索引中不存在版本号小于安全点的条目。
    /// 因此我们可以安全地关闭这些文件句柄，并删除过时的文件。
    fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_gen = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= first_gen {
                break;
            }
            readers.remove(&first_gen);
        }
    }   

    /// 根据给定'OperationPos'读取日志文件
    fn read_and<F, R>(&self, op_pos: OperationPos, f: F) -> Result<R>
    where 
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        self.close_stale_handles();

        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&op_pos.gen) {
            let reader = BufReaderWithPos::new(File::open(log_path(&self.path, op_pos.gen))?)?;
            readers.insert(op_pos.gen, reader);
        }
        let reader = readers.get_mut(&op_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(op_pos.pos))?;
        let op_reader = reader.take(op_pos.len);
        f(op_reader)
    }

    // 根据给定'OperationPos'读取日志文件并反序列化为'Operation'.
    fn read_operation(&self, op_pos: OperationPos) -> Result<Operation> {
        self.read_and(op_pos, |op_reader| {
            Ok(serde_json::from_reader(op_reader)?)
        })
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            // 创建新的读取器，不共享偏移量等底层数据
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    // 可以被压缩删除的冗余操作大小，按字节计数
    uncompacted: u64,
    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, OperationPos>>,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set { key, value };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &op)?;
        self.writer.flush()?;
        if let Operation::Set { key, .. } = op {
            if let Some(old_op) = self.index.get(&key) {
                self.uncompacted += old_op.value().len;
            }
            self.index
                .insert(key, (self.current_gen, pos..self.writer.pos).into());
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let op = Operation::Rm { key };
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &op)?;
            self.writer.flush()?;
            if let Operation::Rm { key } = op {
                let old_op = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_op.value().len;
                // "remove"命令本身也可以在压缩操作时被删除
                self.uncompacted += self.writer.pos - pos;
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// 删除冗余日志
    fn compact(&mut self) -> Result<()> {
        // 当前版本号加二。其中一个是由于压缩文件
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = new_log_file(&self.path, self.current_gen)?;

        let mut compaction_writer = new_log_file(&self.path, compaction_gen)?;

        let mut new_pos = 0; // 新日志文件中的偏移量
        for entry in self.index.iter() {
            let len = self.reader.read_and(*entry.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut compaction_writer)?)
            })?;
            self.index.insert(entry.key().clone(), (compaction_gen, new_pos..new_pos + len).into());
            new_pos += len;
        }
        compaction_writer.flush()?;

        self.reader
            .safe_point
            .store(compaction_gen, Ordering::SeqCst);
        self.reader.close_stale_handles();


        // 删除冗余日志文件
        // 注意：实际上这些文件并不会被立即删除，因为 KvStoreReader 仍然持有已打开的文件句柄。
        // 当 KvStoreReader 下次被使用时，它会清理自己持有的过期文件句柄。

        let stale_gens = sorted_gen_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compaction_gen);
        for stale_gen in stale_gens {
            let file_path = log_path(&self.path, stale_gen);
            if let Err(e) = fs::remove_file(&file_path) {
                println!("{:?} cannot be deleted: {}", file_path, e);
            }
        }
        self.uncompacted = 0;

        Ok(())
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
#[derive(Debug, Clone, Copy)]
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

/// 根据给定编号生成日志文件，返回该日志的写入器
fn new_log_file(path: &Path, gen: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, gen);
    let writer = BufWriterWithPos::new(OpenOptions::new().create(true).append(true).open(&path)?)?;

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
    index: &SkipMap<String, OperationPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Operation>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Operation::Set { key, .. } => {
                if let Some(old_cmd) = index.get(&key) {
                    uncompacted += old_cmd.value().len;
                }
                index.insert(key, (gen, pos..new_pos).into());
            }
            Operation::Rm { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.value().len;
                }
                // "remove"命令本身也可以被压缩删除
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

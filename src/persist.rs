use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::ops::Range;
use std::io::{BufReader, BufWriter, Seek, Read, Write, SeekFrom, self};
use std::path::{PathBuf, Path};
use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use crate::Result;

/// 保存在磁盘上的操作
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Operation {
    /// 设置键值对
    Set { 
        /// 键
        key: String, 
        /// 值
        value: String 
    },
    /// 删除键
    Rm { 
        /// 键
        key: String 
    },
}

/// 记录操作在log文件中的位置及长度
pub struct OperationPos {
    /// 编号
    pub gen: u64,
    /// 偏移量
    pub pos: u64,
    /// 长度
    pub len: u64
}

impl From<(u64, Range<u64>)> for OperationPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        OperationPos { gen: gen, pos: range.start, len: range.end - range.start }
    }
}

/// 根据给定编号生成日志文件，并将读取器加入映射，返回该日志的写入器
pub fn new_log_file(path: &Path, gen: u64, readers: &mut HashMap<u64, BufReaderWithPos<File>>) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?,
    )?;

    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

/// 返回给定目录中log文件的有序编号数组
pub fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
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
pub fn load(gen: u64, reader: &mut BufReaderWithPos<File>, index: &mut BTreeMap<String, OperationPos>) -> Result<u64> {
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
pub fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// 附带偏移量的BufReader
pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    /// 读取器偏移量
    pub pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    /// BufReaderWithPos的构造器
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos { reader: BufReader::new(inner), pos: pos })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R>  {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R>  {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

/// 附带偏移量的BufWriter
pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    /// 写入器偏移量
    pub pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    /// BufWriterWithPos的构造器
    pub fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos { writer: BufWriter::new(inner), pos: pos })
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
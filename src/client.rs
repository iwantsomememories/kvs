use std::net::{TcpStream, SocketAddr};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::io::{BufReader, BufWriter, Write};
use crate::error::Result;
use crate::common::*;
use crate::error::KvsError;

/// Kvs客户端
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}


impl KvsClient {
    /// 连接给定addr，生成Kvs客户端
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let reader = TcpStream::connect(addr)?;
        let writer = reader.try_clone()?;

        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(reader)),
            writer: BufWriter::new(writer),
        })
    }

    /// 从服务器获取给定键对应值
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        let resp = GetResponse::deserialize(&mut self.reader)?;

        match resp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    /// 删除服务器上的给定键
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Rm { key })?;
        self.writer.flush()?;
        let resp = RmResponse::deserialize(&mut self.reader)?;

        match resp {
            RmResponse::Ok(_) => Ok(()),
            RmResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    /// 设置服务器上的键值对
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        let resp = RmResponse::deserialize(&mut self.reader)?;

        match resp {
            RmResponse::Ok(_) => Ok(()),
            RmResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }
}
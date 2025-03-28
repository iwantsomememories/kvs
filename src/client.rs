use std::net::{TcpStream, IpAddr, Ipv4Addr, SocketAddr};
use crate::error::Result;

/// Kvs客户端
pub struct KvsClient {
    connection: TcpStream,
}


impl KvsClient {
    /// 连接给定addr，生成Kvs客户端
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        
        Ok(KvsClient { connection: stream })
    }
}
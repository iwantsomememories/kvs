use crate::engines::KvsEngine;
use crate::error::Result;

use std::net::{TcpListener, TcpStream, SocketAddr};
use slog::Logger;

/// Kvs服务器
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// 根据给定存储引擎生成一个Kvs服务器
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    /// 运行监听给定addr的Kvs服务器
    pub fn run(mut self, addr: SocketAddr, logger: &Logger) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let client_addr = stream.peer_addr()?;
                    info!(logger, "Received a connection from {}", client_addr);
                    let connection_log = logger.new(o!("client addr" => client_addr));
                    if let Err(e) = self.serve(stream) {
                        error!(connection_log, "Error on serving client: {}", e);
                    }
                }
                Err(e) => error!(logger, "Connection failed: {}", e),
            }
        }

        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        // todo!()
        Ok(())
    }
}



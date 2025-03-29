use crate::engines::KvsEngine;
use crate::error::Result;
use crate::common::*;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{BufReader, BufWriter, Write};
use serde_json::de::Deserializer;
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
                    if let Err(e) = self.serve(stream, logger) {
                        error!(connection_log, "Error on serving client: {}", e);
                    }
                }
                Err(e) => error!(logger, "Connection failed: {}", e),
            }
        }

        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream, logger: &Logger) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let tcp_cloned = tcp.try_clone()?;
        let req_reader = Deserializer::from_reader(BufReader::new(&tcp)).into_iter::<Request>();
        let mut writer = BufWriter::new(&tcp_cloned);

        for req in req_reader {
            let req = req?;
            debug!(logger, "Receive request from {}: {:?}", peer_addr, req);

            macro_rules! send_resp {
                ($resp:expr) => {
                    {
                        let resp = $resp;
                        serde_json::to_writer(&mut writer, &resp)?;
                        writer.flush()?;
                        debug!(logger, "Response sent to the {}: {:?}", peer_addr, resp);
                    }
                };
            }

            match req {
                Request::Get { key } => send_resp!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Rm { key } => send_resp!(match self.engine.remove(key) {
                    Ok(_) => RmResponse::Ok(()),
                    Err(e) => RmResponse::Err(format!("{}", e))
                }),
                Request::Set { key, value } => send_resp!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e))
                }),
            }
        }

        Ok(())
    }
}



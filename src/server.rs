use crate::engines::KvsEngine;
use crate::error::Result;
use crate::common::*;
use crate::thread_pool::ThreadPool;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{BufReader, BufWriter, Write};
use std::sync::Arc;
use serde_json::de::Deserializer;
use slog::Logger;

/// Kvs服务器
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// 根据给定存储引擎生成一个Kvs服务器
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    /// 运行监听给定addr的Kvs服务器
    pub fn run(self, addr: SocketAddr, logger: Arc<Logger>) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            let engine = self.engine.clone();
            let connection_logger = logger.clone();

            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = serve(engine.clone(), stream, connection_logger.clone()) {
                        error!(connection_logger, "Error on serving client: {}", e);
                    }
                }
                Err(e) => error!(connection_logger, "Connection failed: {}", e),
            });
        }

        Ok(())
    }
}

fn serve<E: KvsEngine>(engine: E, tcp: TcpStream, logger: Arc<Logger>) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let tcp_cloned = tcp.try_clone()?;
    let req_reader = Deserializer::from_reader(BufReader::new(&tcp)).into_iter::<Request>();
    let mut writer = BufWriter::new(&tcp_cloned);

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

    for req in req_reader {
        let req = req?;
        debug!(logger, "Receive request from {}: {:?}", peer_addr, req);

        match req {
            Request::Get { key } => send_resp!(match engine.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(e) => GetResponse::Err(format!("{}", e)),
            }),
            Request::Rm { key } => send_resp!(match engine.remove(key) {
                Ok(_) => RmResponse::Ok(()),
                Err(e) => RmResponse::Err(format!("{}", e))
            }),
            Request::Set { key, value } => send_resp!(match engine.set(key, value) {
                Ok(_) => SetResponse::Ok(()),
                Err(e) => SetResponse::Err(format!("{}", e))
            }),
        }
    }

    Ok(())
}



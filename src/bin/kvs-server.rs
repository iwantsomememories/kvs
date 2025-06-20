use clap::{Parser, ValueEnum};
use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use serde::{Deserialize, Serialize};
use num_cpus;
use std::fmt::Display;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::env;
use std::env::current_dir;
use std::process::exit;
use std::fs::{File, OpenOptions};
use std::sync::Arc;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use slog::{Drain, Logger};

use kvs::{KvStore, KvsEngine, SledEngine, KvsServer, Result};

const DEFAULT_LISTENING_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);
const DEFAULT_STORAGE_ENGINE: Engine = Engine::Kvs;
const ENGINE_FILE_SUFFIX: &str = ".engine";

#[derive(Debug, Parser)]
#[command(name = env!("CARGO_PKG_NAME"), 
        version = env!("CARGO_PKG_VERSION"), 
        author = env!("CARGO_PKG_AUTHORS"), 
        about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[arg(short, long, default_value_t = DEFAULT_LISTENING_ADDRESS, value_parser = addr_parser)]
    addr: SocketAddr,

    #[arg(short, long, value_enum)]
    engine: Option<Engine>,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug, Serialize, Deserialize)]
enum Engine {
    /// KvStore引擎
    Kvs,
    /// sled引擎
    Sled,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            &Engine::Kvs => write!(f, "kvs"),
            &Engine::Sled => write!(f, "sled"),
        }
    }
}

fn addr_parser(s: &str) -> std::result::Result<SocketAddr, String> {
    match SocketAddr::from_str(s) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(String::from("Invalid addr")),
    }
} 

/// 运行kvs_server
/// # Usages
/// kvs-server [--addr IP-PORT] [--engine ENGINE-NAME]
fn main() {
    let decorator = slog_term::PlainDecorator::new(std::io::stderr());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let server_logger = slog::Logger::root(drain, o!("kvs-server version" => env!("CARGO_PKG_VERSION")));
    let server_logger = Arc::new(server_logger); 

    let cli = Cli::parse();
    info!(server_logger, "Listening on {}", cli.addr; "IP address" => cli.addr.ip().to_string(), "port" => cli.addr.port().to_string());

    let cur_engine = match current_engine() {
        Ok(eng) => eng,
        Err(e) => {
            warn!(server_logger, "The content of engine file is invalid: {e}");
            None
        }
    };

    if cli.engine.is_some() && cur_engine.is_some() && cli.engine != cur_engine {
        error!(server_logger, "Wrong engine!");
        drop(server_logger);
        exit(1);
    }

    let engine = cli.engine.unwrap_or(DEFAULT_STORAGE_ENGINE);
    info!(server_logger, "Storage Engine: {}", engine; "storage engine" => format!("{}", engine));

    let res = run(engine, cli.addr, server_logger.clone());
    if let Err(e) = res {
        error!(server_logger, "{}", e);
        drop(server_logger);
        exit(1);
    }
}

fn run(engine: Engine, addr: SocketAddr, logger: Arc<Logger>) -> Result<()> {
    let engine_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(current_dir()?.join(ENGINE_FILE_SUFFIX))?;

    serde_json::to_writer(engine_file, &engine)?;

    let pool = NaiveThreadPool::new(num_cpus::get() as u32)?;

    match engine {
        Engine::Kvs => run_with_engine(KvStore::open(current_dir()?)?, pool, addr, logger),
        Engine::Sled => run_with_engine(SledEngine::new(sled::open(current_dir()?)?), pool, addr, logger),
    }
}

fn run_with_engine<E: KvsEngine, P: ThreadPool>(engine: E, pool: P, addr: SocketAddr, logger: Arc<Logger>) -> Result<()> {

    let server = KvsServer::new(engine, pool);
    server.run(addr, logger)
}

fn current_engine() -> Result<Option<Engine>>{
    let engine_path = current_dir()?.join(ENGINE_FILE_SUFFIX);
    if !engine_path.exists() {
        return Ok(None);
    }

    let engine_file = File::open(engine_path)?;

    match serde_json::from_reader(engine_file)? {
        Engine::Kvs => return Ok(Some(Engine::Kvs)),
        Engine::Sled => return Ok(Some(Engine::Sled))
    }
}
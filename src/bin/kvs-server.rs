use clap::{Parser, ValueEnum};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;
use std::process::exit;

#[derive(Debug, Parser)]
#[command(name = env!("CARGO_PKG_NAME"), 
        version = env!("CARGO_PKG_VERSION"), 
        author = env!("CARGO_PKG_AUTHORS"), 
        about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000), value_parser = addr_parser)]
    addr: SocketAddr,

    #[arg(long, value_enum)]
    engine: Option<Engine>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Engine {
    /// KvStore引擎
    Kvs,
    /// sled引擎
    Sled,
}

fn addr_parser(s: &str) -> std::result::Result<SocketAddr, String> {
    match SocketAddr::from_str(s) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(String::from("Invalid addr")),
    }
} 

fn main() {
    let cli = Cli::parse();

    println!("addr: {}", cli.addr);

    match cli.engine {
        Some(Engine::Kvs) | None => println!("engine: KvStore"),
        Some(Engine::Sled) => println!("engine: sled"),
    }
}
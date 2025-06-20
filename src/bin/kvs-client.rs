use clap::{Parser, Subcommand};
use kvs::{KvsClient, Result};
use std::process::exit;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

const DEFAULT_CONNECT_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);

#[derive(Debug, Parser)]
#[command(name = env!("CARGO_PKG_NAME"), 
        version = env!("CARGO_PKG_VERSION"), 
        author = env!("CARGO_PKG_AUTHORS"), 
        about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, global = true, default_value_t = DEFAULT_CONNECT_ADDRESS, value_parser = addr_parser)]
    addr: SocketAddr,
}

fn addr_parser(s: &str) -> std::result::Result<SocketAddr, String> {
    match SocketAddr::from_str(s) {
        Ok(addr) => Ok(addr),
        Err(_) => Err(String::from("Invalid addr")),
    }
} 

#[derive(Subcommand, Debug)]
enum Commands {
    /// 设置键值
    Set { key: String, value: String },

    /// 获取键值
    Get { key: String },

    /// 删除键
    Rm { key: String },
}

/// 运行kvs_client
/// # Usages
/// kvs-client set <KEY> <VALUE> [--addr IP-PORT]
/// kvs-client get <KEY> [--addr IP-PORT]
/// kvs-client rm <KEY> [--addr IP-PORT]
#[allow(unused_variables)]
fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Set { key, value } => {
            let mut client = KvsClient::connect(cli.addr)?;
            client.set(key, value)?;
        }
        Commands::Get { key } => {
            let mut client = KvsClient::connect(cli.addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Commands::Rm { key } => {
            let mut client = KvsClient::connect(cli.addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}

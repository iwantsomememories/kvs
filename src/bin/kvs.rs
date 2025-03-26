use clap::{Parser, Subcommand};
use kvs::{KvStore, KvsError, Result};
use std::process::exit;
use std::env::current_dir;

#[derive(Debug, Parser)]
#[command(name = env!("CARGO_PKG_NAME"), 
        version = env!("CARGO_PKG_VERSION"), 
        author = env!("CARGO_PKG_AUTHORS"), 
        about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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

#[allow(unused_variables)]
fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Set { key, value } => {
            let mut kvs = KvStore::open(current_dir()?)?;
            kvs.set(key, value)?;
        }
        Commands::Get { key } => {
            let mut kvs = KvStore::open(current_dir()?)?;
            if let Some(value) = kvs.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Commands::Rm { key } => {
            let mut kvs = KvStore::open(current_dir()?)?;
            match kvs.remove(key) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}

use clap::{Parser, Subcommand};
use std::process;

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
fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Set { key, value } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        Commands::Get { key } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        Commands::Rm { key } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
    }
}

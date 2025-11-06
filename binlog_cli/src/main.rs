mod cli_client;
mod cli_options;

use std::env::current_dir;
use std::fmt::{Debug};
use std::path::PathBuf;
use std::process;
use clap::{Parser, Subcommand};
use serde::Serialize;

use common::config::{BinlogConfig, FConfig, read_config};
use common::config::load_style::Format;
use common::err::CResult;
use common::log::tracing_factory::{OutputType, TracingFactory, TracingFactoryOptions};
use common::pretty_util::to_string_pretty;
use common::server::{Server};
use crate::cli_client::{CliClient};
use crate::cli_options::CliOptions;

#[derive(Parser, Serialize, Debug, Clone)]
#[command(name = "binlog_cli")]
#[command(version)]
#[command(author = "rust-us <yueny09@163.com>")]
#[command(about = "MySQL binlog tool impl with Rust")]
#[command(long_about = "A high-performance MySQL binlog parser and reader tool written in Rust.
Supports real-time binlog streaming, multiple output formats, and flexible configuration options.")]
pub(crate) struct CliArgs {
    /// 加载的配置文件路径
    /// #[arg(short, long)]的作用是为 config 参数设置单字母选项和长选项. 设置#[arg]后会将name放在Option选项中(变成了相当于关键字参数).
    #[arg(short, long, help = "Path to loaded configuration file", value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(long, help = "shut down binlog cli")]
    pub stop: bool,

    ///////////////////////////////////////////////////
    // Cli Options //
    ///////////////////////////////////////////////////
    /// enable debug info
    #[arg(short, long, help = "enable debug mode", default_value_t = false)]
    pub debug: bool,

    #[arg(short, long, help = "output format: [yaml | json], default Yaml", default_value = "yaml")]
    pub format: String,

    ///////////////////////////////////////////////////
    // Binlog Options //
    ///////////////////////////////////////////////////
    #[arg(long = "host", help = "mysql host", value_name = "host")]
    pub host: Option<String>,

    #[arg(long = "port", help = "mysql port, [1-65555]", value_name = "port")]
    pub port: Option<i16>,

    #[arg(short, long = "username", help = "mysql username", value_name = "username")]
    pub username: Option<String>,

    #[arg(short, long = "password", help = "mysql password", value_name = "password")]
    pub password: Option<String>,


    ///////////////////////////////////////////////////
    // Just for test //
    ///////////////////////////////////////////////////
}

// must declared as private
#[derive(Subcommand, Serialize, Debug, Clone)]
enum Commands {
    // Usage: binlog_cli timestamp <TIMESTAMP>
    Timestamp {
        timestamp: String
    }
}

#[tokio::main]
async fn main() -> CResult<()> {
    let args = CliArgs::parse();
    
    // 处理停止命令
    if args.stop {
        println!("Stopping binlog CLI...");
        process::exit(0);
    }

    let format = Format::format(&args.format);
    
    if args.debug {
        eprintln!("args: \n{} ", to_string_pretty(&format, &args));
    }

    // 加载配置
    let config = match load_config(&args) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error loading config: {}", e);
            process::exit(1);
        }
    };
    
    let rep_config = config.get_config();
    
    if args.debug {
        eprintln!("load config: \n{}", to_string_pretty(&format, &rep_config));
    }

    // 初始化日志
    let log_opt = TracingFactoryOptions::new(args.debug, OutputType::LOG, rep_config.base.get_log_dir());
    let log_factory = TracingFactory::init_log_with_options(log_opt);
    
    if args.debug {
        eprintln!("log_dir: {:?}", log_factory.get_log_dir());
    }

    let mut binlog_config = rep_config.binlog;

    if args.debug {
        eprintln!("load binlog config: \n{}", to_string_pretty(&format, &binlog_config));
    }

    // 合并 binlog 设置
    if let Err(e) = merge(&mut binlog_config, &args) {
        eprintln!("Error merging binlog config: {}", e);
        process::exit(1);
    }

    if args.debug {
        eprintln!("final binlog config: {}", to_string_pretty(&format, &binlog_config));
    }

    // 显示启动横幅
    print_banner(args.debug);

    // 验证配置
    if let Err(e) = validate_config(&binlog_config) {
        eprintln!("Configuration validation failed: {}", e);
        process::exit(1);
    }

    // 创建并启动客户端
    let mut client = CliClient::new(CliOptions::new_with_log(args.debug, format), binlog_config);
    
    match client.start().await {
        Ok(_) => {
            println!("Binlog CLI started successfully");
            
            // 等待中断信号
            tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
            println!("\nReceived interrupt signal, shutting down...");
            
            if let Err(e) = client.shutdown(true).await {
                eprintln!("Error during shutdown: {}", e);
                process::exit(1);
            }
            
            println!("Binlog CLI stopped");
        }
        Err(e) => {
            eprintln!("Failed to start binlog CLI: {}", e);
            process::exit(1);
        }
    }

    Ok(())
}

// 加载配置文件， 读取配置
fn load_config(args: &CliArgs) -> CResult<FConfig> {
    let config_path = get_config_path(&args);

    let config = if let Some(path) = config_path {
        if path.exists() {
            match read_config(path) {
                Ok(config) => FConfig::new(config),
                Err(e) => {
                    eprintln!("Failed to read config file: {}", e);
                    return Err(e);
                }
            }
        } else {
            eprintln!("Config file not found: {:?}, using default config", path);
            FConfig::default()
        }
    } else {
        FConfig::default()
    };

    Ok(config)
}

fn get_config_path(args: &CliArgs) -> Option<PathBuf> {
    let path = {
        if args.config.is_some() {
            return Some(args.config.as_ref().unwrap().clone());
        }

        let mut pwd = current_dir().unwrap_or("/".into());
        // ./conf/replayer.toml
        pwd.push("conf");
        pwd.push("replayer");
        pwd.set_extension("toml");

        Some(pwd)
    };

    path
}

fn merge(binlog_config: &mut BinlogConfig, args: &CliArgs) -> CResult<bool> {
    // 合并主机配置
    if let Some(ref host) = args.host {
        binlog_config.set_host(Some(host.clone()));
    } else if !binlog_config.have_host() {
        binlog_config.set_host(Some("127.0.0.1".to_string()));
    }

    // 合并端口配置
    if let Some(port) = args.port {
        binlog_config.set_port(Some(port));
    } else if !binlog_config.have_port() {
        binlog_config.set_port(Some(3306));
    }

    // 合并用户名配置
    if let Some(ref username) = args.username {
        binlog_config.username = username.clone();
    }

    // 合并密码配置
    if let Some(ref password) = args.password {
        binlog_config.password = password.clone();
    }

    Ok(true)
}

/// 显示启动横幅
fn print_banner(debug: bool) {
    let debug_flag = if debug { " [DEBUG]" } else { "" };
    
    eprintln!();
    eprintln!("╔╦╗╔═╗ ╔═╗╔╦╗╦  ");
    eprintln!(" ║ ╠═╣ ║   ║ ║  ");
    eprintln!(" ╩ ╩ ╩ ╚═╝ ╩ ╩═╝ Rust MySQL Binlog CLI v0.0.3{}", debug_flag);
    eprintln!();
}

/// 验证配置
fn validate_config(config: &BinlogConfig) -> CResult<()> {
    if config.username.is_empty() {
        return Err(common::err::decode_error::ReError::Error("Username is required".to_string()));
    }
    
    if config.password.is_empty() {
        return Err(common::err::decode_error::ReError::Error("Password is required".to_string()));
    }
    
    Ok(())
}

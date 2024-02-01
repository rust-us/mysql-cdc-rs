mod cli_client;
mod cli_options;
mod pretty_util;

use std::fmt::{Debug};
use std::path::PathBuf;
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use connection::binlog::lifecycle::lifecycle::BinlogLifecycle;
use common::config::{BinlogConfig, read_config};
use common::err::CResult;
use common::err::decode_error::ReError;
use common::log::tracing_factory::TracingFactory;
use common::server::{Server, ShutdownHandle};
use crate::cli_client::{CliClient, conver_format, to_string_pretty};
use crate::cli_options::CliOptions;

#[derive(Parser, Serialize, Debug, Clone)]
#[command(name = "cdc-cli")]
#[command(version = "0.0.1")]
#[command(author = "rust-us")]
// about [=<expr>] 启用但未设置值时, crate description. 未启用时为Doc comment
#[command(about = "MySQL binlog tool impl with Rust")]
// long_about [=<expr>] 启用但未设置值时, 使用Doc comment. 未启用时没有值
#[command(long_about = None)]
pub(crate) struct CliArgs {
    /// 加载的配置文件路径
    /// #[arg(short, long)]的作用是为 config 参数设置单字母选项和长选项. 设置#[arg]后会将name放在Option选项中(变成了相当于关键字参数).
    #[arg(short, long, help = "Path to loaded configuration file", value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(long, help = "shut down binlog cli")]
    pub stop: Option<bool>,

    ///////////////////////////////////////////////////
    // Cli Options //
    ///////////////////////////////////////////////////
    /// enable debug info
    #[arg(short, long, help = "enable debug mode", default_value_t = false)]
    pub debug: bool,

    #[arg(short, long, help = "output format: [yaml | json]")]
    pub format: Option<String>,


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
}

// must declared as private
#[derive(Subcommand, Serialize, Debug, Clone)]
enum Commands {
    // Usage: binlog_cli timestamp <TIMESTAMP>
    Timestamp {
        timestamp: String
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum Format {
    Json,

    Yaml,

    None,
}

impl TryFrom<&str> for Format {
    type Error = (ReError);

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "yaml" => {
                Ok(Format::Yaml)
            },
            "json" => {
                Ok(Format::Json)
            },
            _ => {
                Err(ReError::String(String::from("Format error")))
            }
        }
    }
}

#[tokio::main]
async fn main() -> CResult<()> {
    let args = CliArgs::parse();
    let format = conver_format(&args.format);

    TracingFactory::init_log(args.debug);
    if args.debug {
        eprintln!("args: \n{}", to_string_pretty(&format, &args));
    }

    let default_conf = get_config_path(&args);
    let mut binlog_config = if default_conf.is_some() {
        let rep_conf = read_config(default_conf.unwrap())?;

        rep_conf.binlog
    } else {
        let mut b = BinlogConfig::default();
        // b.host = "192.168.42.237".to_string();
        b.username = "root".to_string();
        b.password = "123456".to_string();
        // b.password = "Aa123456".to_string();

        b
    };

    if args.debug {
        eprintln!("default_binlog_config: \n{}", to_string_pretty(&format, &binlog_config));;
    }

    // merge binlog settings
    merge(&mut binlog_config, &args).expect("merge default binlog_config and args error!");

    if args.debug {
        eprintln!("final binlog_config: \n{}", to_string_pretty(&format, &binlog_config));
    }

    eprintln!();
    eprintln!("╔╦╗╔═╗ ╔═╗╔╦╗╦  ");
    eprintln!(" ║ ╠═╣ ║   ║ ║  ");
    eprintln!(" ╩ ╩ ╩ ╚═╝ ╩ ╩═╝ Rust us Binlog CLI");
    eprintln!();

    let mut client = CliClient::new(CliOptions::new(args.debug, format), binlog_config);
    client.start().await?;

    // let mut shundown = ShutdownHandle::create();
    // shundown.add_service(Box::new(client));

    Ok(())
}

fn get_config_path(args: &CliArgs) -> Option<PathBuf> {
    let path = {
        if args.config.is_some() {
            return Some(args.config.as_ref().unwrap().clone());
        }

        None
        // let pwd = current_dir().unwrap_or("/".into());
        //
        // let path_ = if pwd.ends_with("/common") {
        //     "../conf/replayer.toml"
        // } else {
        //     "./conf/replayer.toml"
        // }.into();
        //
        // Some(path_)
    };

    path
}

fn merge(binlog_config: &mut BinlogConfig, args: &CliArgs) -> CResult<bool> {
    if args.host.is_some() {
        binlog_config.host = args.host.as_ref().unwrap().clone();
    }

    if args.port.is_some() {
        binlog_config.port = args.port.as_ref().unwrap().clone();
    }

    if args.username.is_some() {
        binlog_config.username = args.username.as_ref().unwrap().clone();
    }

    if args.password.is_some() {
        binlog_config.password = args.password.as_ref().unwrap().clone();
    }

    Ok(true)
}

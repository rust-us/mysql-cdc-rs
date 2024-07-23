mod cli_client;
mod cli_options;

use std::env::current_dir;
use std::fmt::{Debug};
use std::path::PathBuf;
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use connection::binlog::lifecycle::lifecycle::BinlogLifecycle;
use common::config::{BinlogConfig, FConfig, read_config};
use common::config::load_style::Format;
use common::err::CResult;
use common::log::tracing_factory::{OutputType, TracingFactory, TracingFactoryOptions};
use common::pretty_util::to_string_pretty;
use common::server::{Server};
use crate::cli_client::{CliClient};
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
    let format = Format::format(&args.format);
    eprintln!("args: \n{} ", to_string_pretty(&format, &args));

    let config = load_config(&args);
    let rep_config = config.get_config();
    eprintln!("load config: \n{}", to_string_pretty(&format, &rep_config));;

    let log_opt = TracingFactoryOptions::new(args.debug, OutputType::LOG, rep_config.base.get_log_dir());
    let log_factory = TracingFactory::init_log_with_options(log_opt);
    // TracingFactory::init_log(args.debug);
    eprintln!("log_dir: {:?}", log_factory.get_log_dir());

    let mut binlog_config = rep_config.binlog;

    if args.debug {
        eprintln!("load binlog config: \n{}", to_string_pretty(&format, &binlog_config));
    }

    // merge binlog settings
    merge(&mut binlog_config, &args).expect("merge default binlog_config and args error!");

    eprintln!("final binlog config: {}", to_string_pretty(&format, &binlog_config));

    let cli_f_d_ = match args.debug {true => {"-d"},false => {""}};
    let cli_f_ = format!("{}", cli_f_d_);
    let cli_f_some_ = format!("[{}]", cli_f_);
    let cli_output = format!("{}", match cli_f_.is_empty() {true => {""},false => {&cli_f_some_}});

    eprintln!();
    eprintln!("╔╦╗╔═╗ ╔═╗╔╦╗╦  ");
    eprintln!(" ║ ╠═╣ ║   ║ ║  ");
    eprintln!(" ╩ ╩ ╩ ╚═╝ ╩ ╩═╝ Rust us Binlog CLI {}", cli_output);
    eprintln!();

    let mut client = CliClient::new(CliOptions::new_with_log(args.debug, format), binlog_config);
    client.start().await?;

    // let mut shundown = ShutdownHandle::create();
    // shundown.add_service(Box::new(client));

    Ok(())
}

// 加载配置文件， 读取配置
fn load_config(args: &CliArgs) -> FConfig {
    let default_conf = get_config_path(&args);

    let _config =
        if default_conf.is_some() {
            let rep_conf_rs = read_config(default_conf.unwrap());

            FConfig::new(rep_conf_rs.unwrap())
        } else {
            FConfig::default()
        };

    _config
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
    if args.host.is_some() {
        binlog_config.set_host(args.host.clone());
    }
    if binlog_config.have_host() {
        binlog_config.set_host(Some("127.0.0.1".to_string()));
    }

    if args.port.is_some() {
        binlog_config.set_port(args.port.clone());
    }
    if binlog_config.have_port() {
        binlog_config.set_port(Some(3306));
    }

    if args.username.is_some() {
        binlog_config.username = args.username.as_ref().unwrap().clone();
    }

    if args.password.is_some() {
        binlog_config.password = args.password.as_ref().unwrap().clone();
    }

    Ok(true)
}

use std::fmt::Debug;
use std::thread;
use serde::Serialize;
use tracing::debug;
use binlog::binlog_server::BinlogServer;
use binlog::events::binlog_event::BinlogEvent;
use common::config::BinlogConfig;
use common::err::decode_error::ReError;
use common::server::{Server};
use connection::binlog::binlog_subscribe::BinlogSubscribe;
use connection::binlog::lifecycle::lifecycle::BinlogLifecycle;
use crate::{Format};
use crate::cli_options::CliOptions;
use crate::pretty_util::{to_bytes_len_pretty, to_duration_pretty};

#[derive(Debug)]
pub struct CliClient {
    cli_options: CliOptions,

    binlog_config: BinlogConfig,

    binlog_server: BinlogServer,

    binlog_subscribe: BinlogSubscribe,
}

unsafe impl Send for CliClient {}

impl CliClient {
    pub fn new(cli_options: CliOptions, binlog_config: BinlogConfig) -> Self {
        let binlog_server = BinlogServer::new();
        let binlog_subscribe= BinlogSubscribe::new(cli_options.is_debug(), binlog_config.clone());

        CliClient {
            cli_options,
            binlog_config,
            binlog_server,
            binlog_subscribe,
        }
    }
}

#[async_trait::async_trait]
impl Server for CliClient {
    async fn start(&mut self) -> Result<(), ReError> {
        println!("CliClient start");

        self.binlog_server.start().await.unwrap();

        binlog_subscribe_start(&mut self.binlog_subscribe, &self.cli_options, &self.binlog_server, &self.binlog_config).await.unwrap();

        let log_pos = self.binlog_subscribe.get_log_position();
        println!("load_read_ptr: [{}], pos {} in {}",
                 self.binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());

        Ok(())
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        println!("CliClient shutdown");

        self.binlog_server.shutdown(graceful).await?;
        self.binlog_subscribe.shutdown(graceful).await?;

        Ok(())
    }
}

async fn binlog_subscribe_start(binlog_subscribe: &mut BinlogSubscribe, cli_options: &CliOptions,
                                binlog_server: &BinlogServer, binlog_config: &BinlogConfig) -> Result<(), ReError> {
    binlog_subscribe.setup(binlog_config).await.unwrap();
    binlog_subscribe.start().await.unwrap();

    // 延缓启动，便于观察上述配置项信息
    debug!("wait for 500 millis to-viewing of the above configuration...");
    let sleep_millis = std::time::Duration::from_millis(500);
    thread::sleep(sleep_millis);

    let mut binlogs_warpper = binlog_subscribe.binlogs().await.unwrap();
    // 读取binlog 数据
    for x in binlogs_warpper.get_iter() {
        if x.is_ok() {
            let list = x.unwrap();

            for e in list {
                let event_type = BinlogEvent::get_type_name(&e);

                if cli_options.is_debug() {
                    let log_pos = binlog_subscribe.get_log_position();
                    println!("[{:?} {}], pos {} in {} \n{:?}\n",
                             event_type, binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name(),
                             to_string_pretty(&cli_options.get_format(), &e));
                } else {
                    if cli_options.is_print_logs() {
                        let log_pos = binlog_subscribe.get_log_position();
                        println!("[{:?} {}], pos {} in {}\n",
                                 event_type, binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());
                    }
                }
            }
        }
    }

    if binlogs_warpper.get_during_time().is_some() {
        println!("binlog 读取完成，耗时：{}， 收包总大小 {} bytes.",
                 to_duration_pretty(&binlogs_warpper.get_during_time().unwrap()),
                 to_bytes_len_pretty(binlogs_warpper.get_receives_bytes()));
    }

    Ok(())
}

pub fn to_string_pretty<T: Sized + Serialize + Debug>(f: &Format, val: &T) -> String {
    match f {
        Format::Json => {
            let serde_json = serde_json::to_string_pretty(val);

            match serde_json {
                Ok(v) => {
                    v
                },
                Err(e) => {
                    format!("to_string_pretty Json error:{:?}", val)
                }
            }
        },
        Format::Yaml => {
            let serde_yaml = serde_yaml::to_string(val);

            match serde_yaml {
                Ok(v) => {
                    v
                },
                Err(e) => {
                    format!("to_string_pretty Yaml error:{:?}", val)
                }
            }
        },
        Format::None => {
            format!("{:?}", val)
        }
    }
}

pub fn conver_format(format: &String) -> Format {
    match format {
        ff => {
            let f = Format::try_from(ff.as_str());

            match f {
                Ok(fff) => {
                    fff
                },
                Err(e) => {
                    Format::Yaml
                }
            }
        }
    }
}

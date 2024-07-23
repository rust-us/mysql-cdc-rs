use std::fmt::Debug;
use std::thread;
use std::time::Duration;
use serde::Serialize;
use tracing::{debug, error, instrument};
use binlog::binlog_server::BinlogServer;
use binlog::events::binlog_event::BinlogEvent;
use binlog::events::log_context::ILogContext;
use binlog::events::log_position::LogFilePosition;
use crate::binlog::lifecycle::lifecycle::BinlogLifecycle;
use common::config::BinlogConfig;
use common::config::load_style::Format;
use common::err::CResult;
use common::err::decode_error::ReError;
use common::pretty_util::{to_bytes_len_pretty, to_duration_pretty, to_string_pretty};
use common::server::Server;
use crate::binlog::binlog_events_wrapper::{BinlogEventsWrapper};
use crate::conn::binlog_connection::{BinlogConnection, IBinlogConnection};
use crate::conn::connection::IConnection;
use crate::conn::connection_options::ConnectionOptions;
use crate::env_options::EnvOptions;

/// Binlog 订阅器
///
///   setup ----> start  -----> binlogs   ---->  pause
///                                       ---->  shutdown
///
#[derive(Debug)]
pub struct BinlogSubscribe {
    debug: bool,

    conn: Option<BinlogConnection>,

    binlog_config: BinlogConfig,
    subscribe_options: SubscribeOptions,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubscribeOptions {
    /// 是否调试模式
    debug: bool,

    /// 是否输出日志
    print_logs: bool,

    format: Format,
}

unsafe impl Send for BinlogSubscribe {}

#[async_trait::async_trait]
impl Server for BinlogSubscribe {
    #[instrument]
    async fn start(&mut self) -> Result<(), ReError> {
        println!("BinlogSubscribe start");

        match self.conn.as_mut().unwrap().try_connect() {
            Ok(rs) => {
                debug!("服务启动成功。尝试连接成功！！！");
            }
            Err(err) => {
                error!("数据库 {} 连接失败!", format!("{}:{}", self.binlog_config.get_host(), self.binlog_config.get_port()));
                return Err(err);
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        println!("BinlogSubscribe shutdown");

        Ok(())
    }
}

#[async_trait::async_trait]
impl BinlogLifecycle for BinlogSubscribe {
    #[instrument]
    async fn setup(&mut self, binlog_config: &BinlogConfig) -> CResult<()> {
        let mut opts = ConnectionOptions::new(
            binlog_config.get_host().to_string(),
            binlog_config.get_port(),
            binlog_config.username.clone(),
            binlog_config.password.clone(),
        );
        opts.set_env(EnvOptions::new(self.debug, false));

        let binlog_conn = BinlogConnection::new(&opts);
        self.conn = Some(binlog_conn);

        Ok(())
    }

    #[instrument]
    async fn binlogs(&mut self) -> Result<BinlogEventsWrapper, ReError> {
        let binlog_event_rs = self.conn.as_mut().unwrap().binlog(self.binlog_config.payload_buffer_size);

        match binlog_event_rs {
            Ok(b) => {
                Ok(b)
            },
            Err(e) => {
                error!("get binlog Events error:{:?}", &e);
                Err(e)
            }
        }
    }

    async fn pause(&mut self) -> CResult<()> {
        todo!()
    }
}

impl BinlogSubscribe {
    pub fn new(debug: bool, binlog_config: BinlogConfig, subscribe_options: SubscribeOptions) -> Self {
        BinlogSubscribe {
            debug,
            conn: None,
            binlog_config,
            subscribe_options,
        }
    }

    /// 当前已经处理的binlog数量
    pub fn load_read_ptr(&self) -> u64 {
        self.conn.as_ref().unwrap().get_log_context().borrow().load_read_ptr()
    }

    /// 获取当前 LogFilePosition
    pub fn get_log_position(&self) -> LogFilePosition {
        self.conn.as_ref().unwrap().get_log_context().borrow().get_log_position()
    }

    pub fn get_binlog_config(&self) -> BinlogConfig {
        self.binlog_config.clone()
    }

    pub async fn binlog_subscribe_start(&mut self,
                                    binlog_config: &BinlogConfig) -> Result<(), ReError> {
        self.setup(binlog_config).await.unwrap();
        self.start().await.unwrap();

        // 延缓启动，便于观察上述配置项信息
        debug!("wait for 500 millis to-viewing of the above configuration...");
        let sleep_millis = std::time::Duration::from_millis(500);
        thread::sleep(sleep_millis);

        let mut binlogs_warpper = self.binlogs().await.unwrap();
        // 读取binlog 数据
        for x in binlogs_warpper.get_iter() {
            if x.is_ok() {
                let list = x.unwrap();

                for e in list {
                    let event_type = BinlogEvent::get_type_name(&e);

                    // 输出事件的详细信息
                    if self.subscribe_options.is_debug() {
                        let log_pos = self.get_log_position();
                        println!("[{:?} {}], pos {} in {} \n{:?}\n",
                                 event_type, self.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name(),
                                 to_string_pretty(&self.subscribe_options.get_format(), &e));
                    } else {
                        // 输出简要信息
                        if self.subscribe_options.is_print_logs() {
                            let log_pos = self.get_log_position();
                            println!("[{:?} {}], pos {} in {}\n",
                                     event_type, self.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());
                        }
                    }
                }
            }
        }

        // 输出耗时信息
        if binlogs_warpper.get_during_time().is_some() {
            println!("binlog 读取完成，耗时：{}， 收包总大小 {} bytes.",
                     to_duration_pretty(&binlogs_warpper.get_during_time().unwrap()),
                     to_bytes_len_pretty(binlogs_warpper.get_receives_bytes()));
        }

        Ok(())
    }
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        SubscribeOptions::new(false, false, Format::None)
    }
}

impl SubscribeOptions {
    pub fn new(debug: bool, print_logs: bool, format: Format) -> Self {
        SubscribeOptions {
            debug,
            print_logs,
            format,
        }
    }

    pub fn is_print_logs(&self) -> bool {
        self.print_logs
    }

    pub fn is_debug(&self) -> bool {
        self.debug
    }

    pub fn get_format(&self) -> Format {
        self.format.clone()
    }
}

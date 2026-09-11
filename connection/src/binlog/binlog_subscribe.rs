use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use serde::Serialize;
use tracing::{debug, error, info, instrument};
use binlog::binlog_server::BinlogServer;
use binlog::events::binlog_event::BinlogEvent;
use binlog::events::log_context::ILogContext;
use binlog::events::log_position::LogFilePosition;
use common::binlog::FIRST_EVENT_POSITION;
use crate::binlog::binlog_options::BinlogOptions;
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

#[async_trait::async_trait]
impl Server for BinlogSubscribe {
    async fn start(&mut self) -> CResult<()> {
        self.setup_and_connect()?;

        // 延缓启动，便于观察上述配置项信息
        debug!("wait for 500 millis to-viewing of the above configuration...");
        let sleep_millis = std::time::Duration::from_millis(500);
        thread::sleep(sleep_millis);

        let stop = Arc::new(AtomicBool::new(false));
        self.forward_events(stop, |_event, _position| Ok(()))?;

        Ok(())
    }

    async fn shutdown(&mut self, graceful: bool) -> CResult<()> {
        println!("BinlogSubscribe shutdown");

        Ok(())
    }
}

#[async_trait::async_trait]
impl BinlogLifecycle for BinlogSubscribe {
    #[instrument]
    fn setup(&mut self, binlog_config: &BinlogConfig) -> CResult<()> {
        let mut opts = ConnectionOptions::new(
            binlog_config.get_host().to_string(),
            binlog_config.get_port(),
            binlog_config.username.clone(),
            binlog_config.password.clone(),
        );
        opts.set_env(EnvOptions::new(self.debug, false));

        // 起始位点: 配置了 binlog 文件名则从指定位点开始复制（断点续传的基础）
        let file_name = binlog_config.file.clone().unwrap_or_default();
        if !file_name.is_empty() {
            let position = binlog_config
                .position
                .unwrap_or(FIRST_EVENT_POSITION as i64)
                .max(FIRST_EVENT_POSITION as i64) as u64;
            info!("binlog replication start from position: {}@{}", file_name, position);
            opts.binlog = Some(Arc::new(RefCell::new(BinlogOptions::from_position(file_name, position))));
        }

        let binlog_conn = BinlogConnection::new(&opts);
        self.conn = Some(binlog_conn);

        Ok(())
    }

    #[instrument]
    async fn binlogs(&mut self) -> Result<BinlogEventsWrapper, ReError> {
        match self.binlogs_sync() {
            Ok(b) => Ok(b),
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

    /// 建立 binlog 连接(setup + connect)。
    pub fn setup_and_connect(&mut self) -> CResult<()> {
        let c = self.get_binlog_config();
        self.setup(&c)?;
        self.start_in()?;
        Ok(())
    }

    /// 设置复制起始位点(断点续传)，需在 `setup_and_connect` 之前调用。
    pub fn set_start_position(&mut self, file_name: String, position: u64) {
        self.binlog_config.file = Some(file_name);
        self.binlog_config.position = Some(position as i64);
    }

    /// 采集 binlog 事件并逐条转交给 handler。
    ///
    /// handler 的第二个参数为该事件处理完成后对应的 binlog 位点
    /// (即下一事件的起始位置)，可直接用于位点持久化 / 断点续传。
    ///
    /// # 背压
    ///
    /// 每读取一个网络批次就**完整**交给 handler 处理，处理完成后才读取下一批，
    /// 因此在途事件数量有界、内存占用恒定，并通过 TCP 接收窗口形成端到端背压
    /// (下游处理慢 -> 不读新包 -> MySQL 发送窗口耗尽 -> 上游降速)。
    ///
    /// `stop` 被置位后会在批次间隙退出，用于优雅停机。
    pub fn forward_events<F>(&mut self, stop: Arc<AtomicBool>, mut handler: F) -> CResult<()>
    where
        F: FnMut(BinlogEvent, LogFilePosition) -> CResult<()>,
    {
        let binlogs_wrapper = self.binlogs_sync()?;

        let mut event_count: u64 = 0;
        for batch in binlogs_wrapper.get_iter() {
            if stop.load(Ordering::SeqCst) {
                info!("stop signal received, binlog subscribe will exit.");
                break;
            }

            let batch = batch?;
            for event in batch {
                self.print_event(&event);
                // 事件解析时已将上下文位点更新为本事件的 end_log_pos
                let log_position = self.get_log_position();
                handler(event, log_position)?;
                event_count += 1;
            }
        }

        // 输出耗时信息
        if let Some(during) = binlogs_wrapper.get_during_time() {
            println!("binlog 读取完成，耗时：{}， 收包总大小 {} bytes.",
                     to_duration_pretty(&during),
                     to_bytes_len_pretty(binlogs_wrapper.get_receives_bytes()));
        }
        info!("binlog subscribe finished, handled {} events.", event_count);

        Ok(())
    }

    /// 按 debug / print_logs 输出事件信息
    fn print_event(&self, event: &BinlogEvent) {
        let event_type = BinlogEvent::get_type_name(event);

        if self.subscribe_options.is_debug() {
            let log_pos = self.get_log_position();
            println!("[{:?} {}], pos {} in {} \n{:?}\n",
                     event_type, self.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name(),
                     to_string_pretty(&self.subscribe_options.get_format(), event));
        } else if self.subscribe_options.is_print_logs() {
            let log_pos = self.get_log_position();
            println!("[{:?} {}], pos {} in {}\n",
                     event_type, self.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());
        }
    }

    /// 同步获取 binlog 事件迭代器
    fn binlogs_sync(&mut self) -> Result<BinlogEventsWrapper, ReError> {
        let conn = self.conn.as_mut()
            .ok_or_else(|| ReError::String("binlog connection not setup.".to_string()))?;
        conn.binlog(self.binlog_config.payload_buffer_size)
    }

    fn start_in(&mut self) -> Result<(), ReError> {
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

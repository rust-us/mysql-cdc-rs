use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use serde::Serialize;
use tracing::{debug, error, info, instrument};
use binlog::binlog_server::BinlogServer;
use binlog::events::binlog_event::BinlogEvent;
use binlog::events::log_context::ILogContext;
use binlog::events::log_position::LogFilePosition;
use common::binlog::FIRST_EVENT_POSITION;
use crate::binlog::binlog_options::BinlogOptions;
use crate::binlog::lifecycle::lifecycle::BinlogLifecycle;
use crate::TIMEOUT_LATENCY_DELTA;
use crate::TIMEOUT_MESSAGE;
use common::config::{BinlogConfig, DEFAULT_BINLOG_HEARTBEAT_INTERVAL_SECS, DEFAULT_BINLOG_SERVER_ID};
use common::config::load_style::Format;
use common::err::CResult;
use common::err::decode_error::ReError;
use common::pretty_util::{to_bytes_len_pretty, to_duration_pretty, to_string_pretty};
use common::server::Server;
use crate::binlog::binlog_events_wrapper::{BinlogEventsWrapper};
use crate::conn::binlog_connection::{BinlogConnection, IBinlogConnection};
use crate::conn::connection::IConnection;
use crate::conn::connection_options::{ClientIdentity, ConnectionOptions, SslOpts};
use crate::conn::ssl_mode::SslMode;
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

    /// 主库心跳间隔(0 表示禁用心跳，同时关闭空闲存活检测)
    heartbeat_interval: Duration,
}

/// 空闲存活看门狗。
///
/// 只要在 `max_idle` 内收到过任意数据(包含主库心跳事件)就认为连接存活；
/// 超过该时长没有任何数据，说明对端已静默失联(例如机器掉电、网络黑洞，
/// 此时 TCP 不会收到 FIN/RST)。
#[derive(Debug)]
struct IdleWatchdog {
    last_data_at: Instant,
    max_idle: Option<Duration>,
}

impl IdleWatchdog {
    fn new(max_idle: Option<Duration>) -> Self {
        IdleWatchdog {
            last_data_at: Instant::now(),
            max_idle,
        }
    }

    /// 收到数据(事件或心跳)时刷新
    fn on_data(&mut self) {
        self.last_data_at = Instant::now();
    }

    /// 超过最大空闲时长时返回已空闲的时间
    fn exceeded(&self) -> Option<Duration> {
        let max_idle = self.max_idle?;
        let idle = self.last_data_at.elapsed();
        if idle > max_idle {
            Some(idle)
        } else {
            None
        }
    }
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

        // 流式订阅: server_id 必须非 0。
        // MySQL 对 server_id=0 的 COM_BINLOG_DUMP 会按"一次性 dump"处理，
        // 追平后立即发送 EOF 并结束流；非 0 才会持续等待新事件(从库语义)。
        let server_id = binlog_config.server_id.unwrap_or(DEFAULT_BINLOG_SERVER_ID);
        opts.blocking = true;
        opts.update_server_id(server_id);
        info!("binlog replication server_id: {}", server_id);

        // 主库心跳: 空闲时探活。配合 IdleWatchdog 可检测对端静默失联，
        // 且心跳事件与数据无关(会被上层跳过)。
        let heartbeat_secs = binlog_config
            .heartbeat_interval_secs
            .unwrap_or(DEFAULT_BINLOG_HEARTBEAT_INTERVAL_SECS);
        self.heartbeat_interval = Duration::from_secs(heartbeat_secs);
        opts.heartbeat_interval = self.heartbeat_interval;
        info!("binlog master heartbeat interval: {:?}", self.heartbeat_interval);

        // 读超时: 空闲时周期性唤醒，使停机信号能被及时响应
        if let Some(millis) = binlog_config.read_timeout_millis {
            if millis > 0 {
                opts.set_read_timeout(Duration::from_millis(millis));
            }
        }

        // SSL/TLS
        self.apply_ssl_options(&mut opts, binlog_config)?;

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
            heartbeat_interval: Duration::from_secs(DEFAULT_BINLOG_HEARTBEAT_INTERVAL_SECS),
        }
    }

    /// 空闲多久未收到任何数据(含心跳)就判定连接已死。
    /// 未启用心跳时返回 `None`，即不做存活检测(允许长时间空闲)。
    fn idle_timeout(&self) -> Option<Duration> {
        if self.heartbeat_interval.is_zero() {
            None
        } else {
            Some(self.heartbeat_interval + TIMEOUT_LATENCY_DELTA)
        }
    }

    /// 主库心跳间隔
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// 按配置装配 SSL 模式与证书选项。
    ///
    /// 未配置 `[binlog.ssl]` 时为 Disabled，行为与以前完全一致(明文)。
    fn apply_ssl_options(&self, opts: &mut ConnectionOptions, binlog_config: &BinlogConfig) -> CResult<()> {
        let Some(ssl) = binlog_config.ssl.as_ref() else {
            return Ok(());
        };

        let mode = SslMode::from_name(ssl.mode.as_deref().unwrap_or_default())?;
        opts.set_ssl_mode(mode);
        if !mode.is_enabled() {
            return Ok(());
        }

        // 先按模式推导默认校验策略，再叠加显式配置
        let mut ssl_opts = SslOpts::for_mode(mode);
        if let Some(path) = &ssl.root_cert_path {
            ssl_opts = ssl_opts.with_root_cert_path(Some(path.clone()));
        }
        if let Some(path) = &ssl.client_pkcs12_path {
            let identity = ClientIdentity::new(path.clone())
                .with_password(ssl.client_pkcs12_password.clone().unwrap_or_default());
            ssl_opts = ssl_opts.with_client_identity(Some(identity));
        }
        if let Some(v) = ssl.skip_domain_validation {
            ssl_opts = ssl_opts.with_danger_skip_domain_validation(v);
        }
        if let Some(v) = ssl.accept_invalid_certs {
            ssl_opts = ssl_opts.with_danger_accept_invalid_certs(v);
        }

        info!(
            "binlog replication ssl mode: {}, root_cert: {:?}, client_identity: {:?}",
            mode.as_str(),
            ssl.root_cert_path,
            ssl.client_pkcs12_path
        );
        opts.set_ssl_opts(ssl_opts);
        Ok(())
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
    /// `stop` 被置位后会在包边界退出，用于优雅停机。
    ///
    /// 空闲(无新事件)时 socket 读超时会被视为"暂无数据"而非错误：
    /// 循环回到顶部重新检查停止标志，因此停机延迟上界约为一次读超时。
    ///
    /// # 对端存活检测
    ///
    /// 开启心跳后，主库空闲时会周期性发送心跳事件。若超过
    /// `心跳间隔 + 网络延迟冗余` 仍收不到任何数据(含心跳)，
    /// 判定对端已静默失联并返回错误——这种故障不会有 FIN/RST，
    /// 只靠 socket 读超时是发现不了的。
    pub fn forward_events<F>(&mut self, stop: Arc<AtomicBool>, mut handler: F) -> CResult<()>
    where
        F: FnMut(BinlogEvent, LogFilePosition) -> CResult<()>,
    {
        let binlogs_wrapper = self.binlogs_sync()?;

        // 空闲存活看门狗(未启用心跳时不做检测)
        let mut watchdog = IdleWatchdog::new(self.idle_timeout());

        let mut event_count: u64 = 0;
        for batch in binlogs_wrapper.get_iter() {
            // 每个包边界检查停止标志：空闲读超时后能及时响应停机
            if stop.load(Ordering::SeqCst) {
                info!("stop signal received, binlog subscribe will exit.");
                break;
            }

            let batch = match batch {
                Ok(batch) => batch,
                // 读超时说明当前空闲：不是故障，用于检查停机标志与对端存活
                Err(e) if e.is_timeout() => {
                    if let Some(idle) = watchdog.exceeded() {
                        error!(
                            "no data received for {:?} (heartbeat interval {:?}), consider master dead",
                            idle, self.heartbeat_interval
                        );
                        return Err(ReError::String(TIMEOUT_MESSAGE.to_string()));
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };

            // 收到任意数据(含心跳)即认为连接存活
            watchdog.on_data();

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

#[cfg(test)]
mod test {
    use std::thread;
    use std::time::Duration;

    use common::config::BinlogConfig;
    use common::config::load_style::Format;

    use super::{BinlogSubscribe, IdleWatchdog, SubscribeOptions};

    /// 空闲存活看门狗: 未配置上限时永不超时；配置后按最近一次数据时间判断。
    #[test]
    fn test_idle_watchdog() {
        // 未启用心跳 => 关闭存活检测
        let disabled = IdleWatchdog::new(None);
        assert!(disabled.exceeded().is_none());

        let mut watchdog = IdleWatchdog::new(Some(Duration::from_millis(40)));
        assert!(watchdog.exceeded().is_none(), "just created, should be alive");

        thread::sleep(Duration::from_millis(80));
        assert!(watchdog.exceeded().is_some(), "should be considered dead after max idle");

        // 收到数据(含心跳)后重新计时
        watchdog.on_data();
        assert!(watchdog.exceeded().is_none(), "should be alive after receiving data");
    }

    /// 心跳间隔决定存活检测上限，且需留出网络延迟冗余。
    #[test]
    fn test_idle_timeout_follows_heartbeat_interval() {
        let subscribe = BinlogSubscribe::new(
            false,
            BinlogConfig::default(),
            SubscribeOptions::new(false, false, Format::None),
        );

        let idle_timeout = subscribe.idle_timeout().expect("heartbeat enabled by default");
        assert!(
            idle_timeout > subscribe.heartbeat_interval(),
            "idle timeout should allow for network latency beyond one heartbeat"
        );
    }
}

use tracing::{debug, error, instrument};
use binlog::events::log_context::ILogContext;
use binlog::events::log_position::LogFilePosition;
use crate::binlog::lifecycle::lifecycle::BinlogLifecycle;
use common::config::BinlogConfig;
use common::err::CResult;
use common::err::decode_error::ReError;
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
            binlog_config.host.as_ref().unwrap().clone(),
            binlog_config.port.as_ref().unwrap().clone(),
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
    pub fn new(debug: bool, binlog_config: BinlogConfig) -> Self {
        BinlogSubscribe {
            debug,
            conn: None,
            binlog_config,
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
}
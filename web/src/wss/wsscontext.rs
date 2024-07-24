use std::cell::RefCell;
use std::sync::Arc;
use binlog::binlog_server::BinlogServer;
use common::config::BinlogConfig;
use common::err::decode_error::ReError;
use common::server::Server;
use connection::binlog::binlog_subscribe::{BinlogSubscribe, SubscribeOptions};

#[derive(Debug)]
pub struct WSSContext {
    binlog_server: BinlogServer,

    binlog_subscribe: BinlogSubscribe,
}

impl Default for WSSContext {
    fn default() -> Self {
        let binlog_config = BinlogConfig::default();

        let binlog_server = BinlogServer::new();
        let binlog_subscribe= BinlogSubscribe::new(false, binlog_config,
                                                   SubscribeOptions::default());

        WSSContext {
            binlog_server,
            binlog_subscribe,
        }
    }
}

unsafe impl Send for WSSContext {}

#[async_trait::async_trait]
impl Server for WSSContext {
    async fn start(&mut self) -> Result<(), ReError> {
        println!("WSS Binlog start");

        self.binlog_server.start().await.unwrap();
        self.binlog_subscribe.start().await.unwrap();

        let log_pos = self.binlog_subscribe.get_log_position();
        println!("load_read_ptr: [{}], pos {} in {}",
                 self.binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());

        Ok(())
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        println!("WSS Binlog shutdown");

        self.binlog_server.shutdown(graceful).await?;
        self.binlog_subscribe.shutdown(graceful).await?;

        Ok(())
    }
}

impl WSSContext {
    pub fn create() -> Self {
        WSSContext::default()
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        true
    }
}
use std::sync::{Arc, Mutex, RwLock};
use binlog::binlog_server::BinlogServer;
use common::config::BinlogConfig;
use common::err::decode_error::ReError;
use common::server::Server;
use connection::binlog::binlog_subscribe::{BinlogSubscribe, SubscribeOptions};
use connection::binlog::lifecycle::lifecycle::BinlogLifecycle;

pub type WssSessionRef = Arc<Mutex<WssSession>>;

#[derive(Debug)]
pub struct WssSession {
    // binlog_server: BinlogServer,
    //
    // binlog_subscribe: BinlogSubscribe,
}

impl Default for WssSession {
    fn default() -> Self {
        let binlog_config = BinlogConfig::default();

        let binlog_server = BinlogServer::new();
        let binlog_subscribe= BinlogSubscribe::new(false, binlog_config,
                                                   SubscribeOptions::default());

        WssSession {
            // binlog_server,
            // binlog_subscribe,
        }
    }
}

#[async_trait::async_trait]
impl Server for WssSession {
    async fn start(&mut self) -> Result<(), ReError> {
        println!("WSS Binlog start");

        // self.binlog_server.start().await?;
        // self.binlog_subscribe.start().await?;

        // let log_pos = self.binlog_subscribe.get_log_position();
        // println!("load_read_ptr: [{}], pos {} in {}",
        //          self.binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());

        Ok(())
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        println!("WSS Binlog shutdown");

        // self.binlog_server.shutdown(graceful).await?;
        // self.binlog_subscribe.shutdown(graceful).await?;

        Ok(())
    }
}

impl WssSession {
    pub fn create() -> Self {
        WssSession::default()
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        true
    }
}
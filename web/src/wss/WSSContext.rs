use std::cell::RefCell;
use std::sync::Arc;
use binlog::binlog_server::BinlogServer;
use common::config::BinlogConfig;
use common::err::decode_error::ReError;
use common::server::Server;
use connection::binlog::binlog_subscribe::{BinlogSubscribe, SubscribeOptions};

#[derive(Debug)]
pub struct WSSContext {
    // client: Arc<CliClient>,

    binlog_server: Arc<RefCell<BinlogServer>>,

    binlog_subscribe: Arc<RefCell<BinlogSubscribe>>,
}

impl Default for WSSContext {
    fn default() -> Self {
        let binlog_config = BinlogConfig::default();

        let binlog_server = BinlogServer::new();
        let binlog_subscribe= BinlogSubscribe::new(false, binlog_config,
                                                   SubscribeOptions::default());

        WSSContext {
            binlog_server: Arc::new(RefCell::new(binlog_server)),
            binlog_subscribe: Arc::new(RefCell::new(binlog_subscribe)),
        }
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

    async fn start(&mut self) -> Result<(), ReError> {
        self.binlog_server.borrow_mut().start().await.unwrap();

        self.binlog_subscribe.borrow_mut()
            .binlog_subscribe_start(&self.binlog_subscribe.borrow().get_binlog_config()).await.unwrap();

        let log_pos = self.binlog_subscribe.get_log_position();
        println!("load_read_ptr: [{}], pos {} in {}",
                 self.binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());

        Ok(())
    }
}
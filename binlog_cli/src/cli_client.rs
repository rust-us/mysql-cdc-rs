use std::fmt::Debug;
use binlog::binlog_server::BinlogServer;
use common::config::BinlogConfig;
use common::err::decode_error::ReError;
use common::server::{Server};
use connection::binlog::binlog_subscribe::BinlogSubscribe;
use crate::cli_options::CliOptions;

#[derive(Debug)]
pub struct CliClient {
    binlog_config: BinlogConfig,

    binlog_server: BinlogServer,

    binlog_subscribe: BinlogSubscribe,
}

impl CliClient {
    pub fn new(cli_options: CliOptions, binlog_config: BinlogConfig) -> Self {
        let binlog_server = BinlogServer::new();
        let binlog_subscribe= BinlogSubscribe::new(
            cli_options.is_debug(),
            binlog_config.clone(),
            cli_options.to_subscribe_options()
        );

        CliClient {
            binlog_config,
            binlog_server,
            binlog_subscribe,
        }
    }
}

unsafe impl Send for CliClient {}

#[async_trait::async_trait]
impl Server for CliClient {
    async fn start(&mut self) -> Result<(), ReError> {
        println!("CliClient start");

        self.binlog_server.start().await.unwrap();
        self.binlog_subscribe.binlog_subscribe_start(&self.binlog_config).await.unwrap();

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
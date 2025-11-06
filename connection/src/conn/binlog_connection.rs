use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use tracing::instrument;
use binlog::alias::mysql::gtid::gtid::Gtid;
use binlog::events::log_context::{ILogContext, LogContext, LogContextRef};
use binlog::events::log_stat::{LogStat, LogStatRef};
use common::err::CResult;
use common::err::decode_error::ReError;
use common::binlog::row::row_string::RowString;
use common::server::Server;
use crate::binlog::binlog_events::BinlogEvents;
use crate::binlog::binlog_events_wrapper::{BinlogEventsWrapper};
use crate::binlog::binlog_options::{BinlogOptions, BinlogOptionsRef};
use crate::binlog::starting_strategy::StartingStrategy;
use crate::commands::dump_binlog_command::DumpBinlogCommand;
use crate::commands::dump_binlog_gtid_command::DumpBinlogGtidCommand;
use crate::conn::connection::{Connection, IConnection};
use crate::conn::connection_options::ConnectionOptions;
use crate::conn::packet_channel::PacketChannel;
use crate::conn::query_result::StreamQueryResult;

pub trait IBinlogConnection: IConnection {

    ///
    ///
    /// # Arguments
    ///
    /// * `payload_buffer_size`:  读取binlog 的缓冲区大小
    ///
    /// returns: Result<BinlogEvents, ReError>
    fn binlog(&mut self, payload_buffer_size: usize) -> CResult<BinlogEventsWrapper>;

}

/// BinlogClient capability
#[derive(Debug)]
pub struct BinlogConnection {
    conn: Connection,

    options: BinlogOptionsRef,

    /// binlog 解析过程中的上下文
    log_context: LogContextRef,

    /// gtid
    mysql_gtid: Option<Gtid>,
    // other gtid ...
}

unsafe impl Send for BinlogConnection {}

impl BinlogConnection {
    pub fn new(options: &ConnectionOptions) -> Self {
        let conn = Connection::new(options.clone());

        let mut binlog_options = Arc::new(RefCell::new(BinlogOptions::from_start()));
        if options.binlog.is_some() {
            binlog_options = options.binlog.clone().unwrap();
        }

        let context = LogContext::default();
        let log_context = Rc::new(RefCell::new(context));

        Self {
            conn,
            log_context,
            options: binlog_options,
            mysql_gtid: None,
        }
    }

    pub fn get_log_context(&self) -> LogContextRef {
        self.log_context.clone()
    }
}

impl BinlogConnection {
    fn replicate_mysql(channel: &mut Arc<RefCell<PacketChannel>>,
                       options: &ConnectionOptions,
                       server_id: u32) -> CResult<()> {

        if options.binlog.is_none() {
            return Err(ReError::ConnectionError(String::from("BinlogOptions is not found")))
        }

        let binlog_ = options.binlog.as_ref().unwrap();

        if binlog_.borrow().starting_strategy == StartingStrategy::FromGtid {
            if let Some(gtid_set) = &binlog_.borrow().gtid_set {
                let command = DumpBinlogGtidCommand::new(
                    server_id,
                    binlog_.borrow().filename.clone(),
                    binlog_.borrow().position,
                );
                channel.borrow_mut().write_packet(&command.serialize(&gtid_set)?, 0)?
            } else {
                return Err(ReError::String("GtidSet was not specified".to_string()));
            }
        } else {
            let command = DumpBinlogCommand::new(
                server_id,
                binlog_.borrow().filename.clone(),
                binlog_.borrow().position,
            );

            channel.borrow_mut().write_packet(&command.serialize()?, 0)?
        }

        Ok(())
    }

}

#[async_trait::async_trait]
impl Server for BinlogConnection {
    async fn start(&mut self) -> Result<(), ReError> {
        todo!()
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        todo!()
    }
}

impl IConnection for BinlogConnection {
    fn try_connect(&mut self) -> CResult<bool> {
        self.conn.try_connect()
    }

    fn query(&mut self, sql: String) -> CResult<Vec<RowString>> {
        self.conn.query(sql)
    }

    fn query_stream(&mut self, sql: String) -> CResult<StreamQueryResult> {
        self.conn.query_stream(sql)
    }
}

impl IBinlogConnection for BinlogConnection {

    #[instrument]
    fn binlog(&mut self, payload_buffer_size: usize) -> CResult<BinlogEventsWrapper> {
        self.try_connect().expect("binlog try_connect");

        // Reset on reconnect
        self.conn.transaction = false;
        self.mysql_gtid = None;

        let channel = self.conn.channel.as_mut().unwrap();
        self.conn.configure.adjust_starting_position(channel)?;
        // update conn log_context#LogPosition
        // let filename = self.conn.options.binlog.as_ref().unwrap().borrow().filename.as_str();
        // let position = self.conn.options.binlog.as_ref().unwrap().borrow().position;
        // self.log_context.borrow_mut().set_log_position(LogPosition::new_with_position(filename, position));

        self.conn.configure.set_master_heartbeat(channel)?;
        let checksum = self.conn.configure.set_master_binlog_checksum(channel)?;

        let server_id = if self.conn.options.blocking {
            self.conn.options.server_id
        } else {
            0
        };

        BinlogConnection::replicate_mysql(&mut channel.clone(), &self.conn.options, server_id)?;

        let binlogs = BinlogEvents::new(channel.clone(), self.log_context.clone(), checksum, payload_buffer_size)?;
        Ok(BinlogEventsWrapper::new(Arc::new(RefCell::new(binlogs))))
    }
}


// 注意：原来的单元测试已移动到 connection/tests/integration_tests.rs
// 这些测试需要真实的 MySQL 服务器，因此更适合作为集成测试运行
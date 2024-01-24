use std::cell::RefCell;
use std::sync::Arc;
use binlog::alias::mysql::gtid::gtid::Gtid;
use common::err::CResult;
use common::row::row_string::RowString;
use crate::binlog::binlog_events::BinlogEvents;
use crate::binlog::binlog_options::BinlogOptions;
use crate::conn::connection::{Connection, IConnection};
use crate::conn::connection_options::ConnectionOptions;

pub trait IBinlogConnection {
    fn binlog(&mut self) -> CResult<BinlogEvents>;
}

/// BinlogClient capability
pub struct BinlogConnection {
    conn: Connection,

    options: Arc<RefCell<BinlogOptions>>,

    /// gtid
    mysql_gtid: Option<Gtid>,
    // other gtid ...
}

impl BinlogConnection {
    pub fn new(options: ConnectionOptions) -> Self {
        let conn = Connection::new(options.clone());

        let mut binlog_options = Arc::new(RefCell::new(BinlogOptions::from_start()));
        if options.binlog.is_some() {
            binlog_options = options.binlog.clone().unwrap();
        }

        Self {
            conn,
            options: binlog_options,
            mysql_gtid: None,
        }
    }
}


impl IConnection for BinlogConnection {
    fn try_connect(&mut self) -> CResult<bool> {
        self.conn.try_connect()
    }

    fn query(&mut self, sql: String) -> CResult<Vec<RowString>> {
        self.conn.query(sql)
    }
}

impl IBinlogConnection for BinlogConnection {

    fn binlog(&mut self) -> CResult<BinlogEvents> {
        self.try_connect().expect("binlog try_connect");

        // Reset on reconnect
        self.conn.transaction = false;
        self.mysql_gtid = None;

        let channel = self.conn.channel.as_mut().unwrap();
        self.conn.configure.adjust_starting_position(channel)?;
        self.conn.configure.set_master_heartbeat(channel)?;
        let checksum = self.conn.configure.set_master_binlog_checksum(channel)?;

        let server_id = if self.conn.options.blocking {
            self.conn.options.server_id
        } else {
            0
        };

        Ok(BinlogEvents::default())
    }
}


#[cfg(test)]
mod test {
    use binlog::factory::event_factory::{EventFactory, EventReaderOption, IEventFactory};
    use crate::conn::binlog_connection::{BinlogConnection, IBinlogConnection};
    use crate::conn::connection::IConnection;
    use crate::conn::connection_options::ConnectionOptions;

    #[test]
    fn test_conn() {
        let mut opts = ConnectionOptions::default();
        opts.update_auth(String::from("root"), String::from("123456"));

        let mut binlog_conn = BinlogConnection::new(opts);
        let channel_rs = binlog_conn.try_connect();
        assert!(channel_rs.is_ok());

        let query = binlog_conn.query(String::from("select 1+ 1")).expect("test_conn error");
        let values = &query[0].as_slice();
        assert_eq!(values[0], "2")
    }

    #[test]
    fn test_binlog() {
        let mut opts = ConnectionOptions::default();
        opts.update_auth(String::from("root"), String::from("123456"));

        let mut binlog_conn = BinlogConnection::new(opts);
        let binlog_event_rs = binlog_conn.binlog();
        assert!(binlog_event_rs.is_ok());

        let binlog_event = binlog_event_rs.unwrap();

        for x in binlog_event {
            if x.is_ok() {
                let list = x.unwrap();
                assert!(list.len() > 0);
            }
        }
    }
}
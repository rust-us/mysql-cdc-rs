use std::cell::RefCell;
use std::sync::Arc;
use serde::Serialize;
use binlog::alias::mysql::gtid::gtid_set::GtidSet;
use common::binlog::FIRST_EVENT_POSITION;
use crate::binlog::starting_strategy::StartingStrategy;

pub type BinlogOptionsRef = Arc<RefCell<BinlogOptions>>;

/// Replication options used when client connects to the server.
#[derive(Debug, Serialize, Clone)]
pub struct BinlogOptions {
    /// Binary log file name.
    /// The value is automatically changed on the RotateEvent.
    /// On reconnect the client resumes replication from the current position.
    pub filename: String,

    /// Binary log file position.
    /// The value is automatically changed when an event is successfully processed by a client.
    /// On reconnect the client resumes replication from the current position.
    pub position: u64,

    /// MySQL Global Transaction ID position to start replication from.
    /// See <a href="https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html">MySQL GTID</a>
    pub gtid_set: Option<GtidSet>,

    /// Gets replication starting strategy.
    pub starting_strategy: StartingStrategy,
}

impl BinlogOptions {
    /// Starts replication from first available binlog on master server.
    pub fn from_start() -> BinlogOptions {
        BinlogOptions {
            filename: String::new(),
            position: FIRST_EVENT_POSITION as u64,
            gtid_set: None,
            starting_strategy: StartingStrategy::FromStart,
        }
    }

    /// Starts replication from last master binlog position
    /// which will be read by BinlogClient on first connect.
    pub fn from_end() -> BinlogOptions {
        BinlogOptions {
            filename: String::new(),
            position: 0,
            gtid_set: None,
            starting_strategy: StartingStrategy::FromEnd,
        }
    }

    /// Starts replication from specified binlog filename and position.
    pub fn from_position(filename: String, position: u64) -> BinlogOptions {
        BinlogOptions {
            filename,
            position,
            gtid_set: None,
            starting_strategy: StartingStrategy::FromPosition,
        }
    }

    /// Starts replication from specified Global Transaction ID.
    pub fn from_gtid(gtid_set: GtidSet) -> BinlogOptions {
        BinlogOptions {
            filename: String::new(),
            position: FIRST_EVENT_POSITION as u64,
            gtid_set: Some(gtid_set),
            starting_strategy: StartingStrategy::FromGtid,
        }
    }

    pub fn update_filename(&mut self, filename: String) -> bool {
        self.filename = filename;

        true
    }

    pub fn update_position(&mut self, position: u64) -> bool {
        self.position = position;

        true
    }
}

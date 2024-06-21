use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::Serialize;
use common::binlog::FIRST_EVENT_POSITION;
use crate::alias::mysql::gtid::gtid_set::GtidSet;

pub type LogFilePositionRef = Arc<LogFilePosition>;

#[derive(Debug, Serialize)]
pub struct LogFilePosition {
    /// binlog file's name
    file_name: String,

    /// position in file
    position: AtomicU64,

    /// gtid 仅在gtid_mode使用，此时file_name和pos无效
    gtid_set: Option<GtidSet>
}

impl Clone for LogFilePosition {
    fn clone(&self) -> Self {
        LogFilePosition {
            file_name: self.get_file_name(),
            position: AtomicU64::new(self.get_position()),
            gtid_set: self.get_gtid_set(),
        }
    }
}

impl Default for LogFilePosition {
    fn default() -> Self {
        LogFilePosition {
            file_name: "".to_string(),
            position: AtomicU64::new(FIRST_EVENT_POSITION as u64),
            gtid_set: None,
        }
    }
}

impl LogFilePosition {
    pub fn new(file_name: &str) -> Self {
        LogFilePosition::new_with_position(file_name, FIRST_EVENT_POSITION as u64)
    }

    pub fn new_with_position(file_name: &str, position: u64) -> Self {
        LogFilePosition {
            file_name: file_name.to_string(),
            position: AtomicU64::new(position),
            gtid_set: None,
        }
    }

    pub fn new_with_gtid(file_name: &str, position: u64, gtid_data: GtidSet) -> Self {
        LogFilePosition {
            file_name: file_name.to_string(),
            position: AtomicU64::new(position),
            gtid_set: Some(gtid_data),
        }
    }

    pub fn get_file_name(&self) -> String {
        self.file_name.clone()
    }

    pub fn set_position(&mut self, pos: u64) {
        // 将值存储到原子整数中
        self.position.store(pos, Ordering::Relaxed);
    }

    pub fn get_position(&self) -> u64 {
        self.position.load(Ordering::Relaxed)
    }

    pub fn get_gtid_set(&self) -> Option<GtidSet> {
        self.gtid_set.clone()
    }
}

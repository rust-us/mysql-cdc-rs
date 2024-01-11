use serde::Serialize;
use crate::events::gtid_set::MysqlGTIDSet;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct LogPosition {
    /// binlog file's name
    file_name:String,

    /// position in file
    position: u64,

    /// gtid 仅在gtid_mode使用，此时file_name和pos无效
    gtid_set: Option<MysqlGTIDSet>
}

impl Default for LogPosition {
    fn default() -> Self {
        LogPosition {
            file_name: "".to_string(),
            position: 0,
            gtid_set: None,
        }
    }
}

impl LogPosition {
    pub fn new(file_name: &str) -> Self {
        LogPosition::new_with_position(file_name, 0)
    }

    pub fn new_with_position(file_name: &str, position: u64) -> Self {
        LogPosition {
            file_name: file_name.to_string(),
            position,
            gtid_set: None,
        }
    }

    pub fn new_with_gtid(file_name: &str, position: u64, gtid_data: MysqlGTIDSet) -> Self {
        LogPosition {
            file_name: file_name.to_string(),
            position,
            gtid_set: Some(gtid_data),
        }
    }

    pub fn new_copy(pos:&LogPosition) -> Self {
        LogPosition {
            file_name: pos.get_file_name(),
            position: pos.get_position(),
            gtid_set: pos.get_gtid_set(),
        }
    }

    pub fn get_file_name(&self) -> String {
        self.file_name.clone()
    }

    pub fn set_position(&mut self, pos: u64) {
        self.position = pos;
    }

    pub fn get_position(&self) -> u64 {
        self.position
    }

    pub fn get_gtid_set(&self) -> Option<MysqlGTIDSet> {
        self.gtid_set.clone()
    }
}

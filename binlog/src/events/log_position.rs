use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct LogPosition {
    /// binlog file's name
    file_name:String,

    /// position in file
    position: u64,
}

impl Default for LogPosition {
    fn default() -> Self {
        LogPosition {
            file_name: "".to_string(),
            position: 0,
        }
    }
}

impl LogPosition {
    pub fn new(file_name: String) -> Self {
        LogPosition {
            file_name,
            position: 0,
        }
    }

    pub fn new_with_position(file_name: String, position: u64) -> Self {
        LogPosition {
            file_name,
            position,
        }
    }

    pub fn new_copy(pos:&LogPosition) -> Self {
        LogPosition {
            file_name: pos.get_file_name(),
            position: pos.get_position(),
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
}
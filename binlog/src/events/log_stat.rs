use std::sync::{Arc, RwLock};
use serde::Serialize;

pub type LogStatRef = Arc<RwLock<LogStat>>;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct LogStat {
    process_count: u64,
}

impl Default for LogStat {
    fn default() -> Self {
        LogStat {
            process_count: 0,
        }
    }
}

impl LogStat {
    pub fn new() -> Self {
        LogStat {
            process_count: 0,
        }
    }

    pub fn add(&mut self) {
        self.process_count += 1;
    }

    pub fn get_process_count(&self) -> u64 {
        self.process_count
    }
}
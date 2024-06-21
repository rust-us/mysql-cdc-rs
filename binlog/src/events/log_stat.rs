use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use serde::Serialize;

pub type LogStatRef = LogStat;

/// 上下文监控的相关信息
#[derive(Debug, Serialize)]
pub struct LogStat {
    /// 已经读取的事件数量
    read_ptr: AtomicU64,

    /// 接受到的流量总大小
    receives_bytes: AtomicUsize,
}

impl Clone for LogStat {
    fn clone(&self) -> Self {
        LogStat {
            read_ptr: AtomicU64::new(self.load_read_ptr()),
            receives_bytes: AtomicUsize::new(self.load_receives_bytes()),
        }
    }
}

impl Default for LogStat {
    fn default() -> Self {
        LogStat::new()
    }
}

impl LogStat {
    pub fn new() -> Self {
        LogStat {
            read_ptr: AtomicU64::new(0),
            receives_bytes: AtomicUsize::new(0),
        }
    }

    pub fn add(&mut self, len: usize) {
        self.receives_bytes.fetch_add(len, Ordering::Relaxed);

        // 添加到当前值，返回之前的值。此操作在溢出时回绕
        self.read_ptr.fetch_add(1, Ordering::Relaxed);
    }

    /// 获取接受到的流量总大小
    pub fn load_receives_bytes(&self) -> usize {
        self.receives_bytes.load(Ordering::Relaxed)
    }

    pub fn load_read_ptr(&self) -> u64 {
        self.read_ptr.load(Ordering::Relaxed)
    }
}
use getset::{Getters, Setters};

use crate::relay_log::RelayLog;

/// 日志存储块.
/// ========================================
/// 字节大小 = 8 + 8 + 4 + {RelayLogSize}.
/// ```txt
/// index: 索引id, 8字节
/// log_size: 日志内容大小, 8字节
/// checksum: 日志内容校验值, 4字节
/// relay_log: 日志内容, 动态大小
/// ```
/// =========================================
#[derive(Debug, Clone, Getters, Setters)]
pub struct StorageEntry {
    // entry索引id
    #[getset(get = "pub")]
    index: u64,

    // 日志内容大小
    #[getset(get = "pub", set = "pub")]
    log_size: u64,

    // 日志内容校验值
    #[getset(get = "pub", set = "pub")]
    checksum: u32,

    // 中继日志实体
    #[getset(get = "pub")]
    relay_log: RelayLog,
}

impl StorageEntry {
    pub fn new(index: u64,
               log_size: u64,
               checksum: u32,
               relay_log: RelayLog) -> Self {
        Self {
            index,
            log_size,
            checksum,
            relay_log
        }
    }
}
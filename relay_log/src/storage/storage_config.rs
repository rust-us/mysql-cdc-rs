use getset::{Getters, Setters};

/// 版本号
pub(crate) const VERSION: u32 = 1;
/// segment文件前缀
pub(crate) const SEGMENT_FILE_PRE: &str = "rlog";
/// segment文件头大小
pub(crate) const SEGMENT_HEADER_SIZE_BYTES: usize = 64;

/// 存储可配项
#[derive(Debug, Clone, Getters, Setters)]
pub struct StorageConfig {
    // 中继日志存储路径
    #[getset(get = "pub", set = "pub")]
    relay_log_dir: String,

    // 每个segment最大值
    #[getset(get = "pub", set = "pub")]
    max_segment_size: u64,

    // 每个segment最多存实体数量
    #[getset(get = "pub", set = "pub")]
    max_segment_entries: u32,

    // segment file buffer size
    #[getset(get = "pub", set = "pub")]
    entry_buffer_num: usize,

    // 是否flush
    #[getset(get = "pub", set = "pub")]
    flush_on_commit: bool,

    // 日志整理周期
    #[getset(get = "pub", set = "pub")]
    compact_interval_millisecond: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            relay_log_dir: "".to_string(),
            // 10M
            max_segment_size: 10 * 1024 * 1024,
            //
            max_segment_entries: 100,
            // 1k个
            entry_buffer_num: 1024,
            flush_on_commit: false,
            // 5min
            compact_interval_millisecond: 5 * 60 * 1000,
        }
    }
}
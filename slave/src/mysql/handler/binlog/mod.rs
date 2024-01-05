mod binlog_handler;
pub mod binlog_stream;

/// master的心跳间隔，单位ms默认10分钟
const MASTER_HEARTBEAT_PERIOD_MILLISECONDS: u32 = 10 * 60 * 1000;

/// binlog文件头长度，每个binlog文件的第一个event需要从该位置开始读取，pos小于文件头长度server将返回异常
const BINLOG_HEADER_SIZE: u64 = 4;

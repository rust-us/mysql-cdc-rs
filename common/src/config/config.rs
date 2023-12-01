
/// Binlog 配置
#[derive(Debug, Clone, Default)]
pub struct BinlogConfig {
    /// binlog 文件的绝对路径
    pub binlog_path: String,

    /// binlog file, 如 mysql-bin.000005
    pub file: String,

    /// binlog file 消费的起始position
    pub position : i32,
}
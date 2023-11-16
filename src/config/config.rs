
/// Binlog 配置
#[derive(Debug, Clone, Default)]
pub struct BinlogConfig {
    /// binlog_path
    #[serde(default = "binlog_path")]
    pub binlog_path: String,
}
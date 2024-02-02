use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use crate::binlog::PAYLOAD_BUFFER_SIZE;

use crate::err::decode_error::ReError;

pub struct Config {
    pub core: RepConfig,

    max_memory: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepConfig {
    pub binlog: BinlogConfig,
    pub rc_mysql: RcMySQL,
    pub rc_metadata: RcMetadata,
    pub core: CoreConfig,
}

/// Binlog 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogConfig {
    pub host: Option<String>,
    pub port: Option<i16>,
    pub username: String,
    pub password: String,

    /// 读取binlog 的缓冲区大小
    pub payload_buffer_size: usize,

    /// binlog file, 如 mysql-bin.000005
    pub file: Option<String>,

    /// binlog file 消费的起始position
    pub position: Option<i32>,

    /// binlog 文件的绝对路径
    pub binlog_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RcMySQL {
    pub addr: Vec<String>,
    pub username: String,
    pub password: String,
    pub raft_stats_fresh_interval_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RcMetadata {
    pub addr: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub metadata_stats_fresh_interval_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CoreConfig {
    max_memory: Option<String>,
}

impl Default for BinlogConfig {
    fn default() -> Self {
        BinlogConfig {
            host: Some("127.0.0.1".to_string()),
            port: Some(3306),
            username: "root".to_string(),
            password: "123456".to_string(),
            payload_buffer_size: PAYLOAD_BUFFER_SIZE,
            file: Some("".to_string()),
            position: Some(4),
            binlog_path: Some("".to_string()),
        }
    }
}

pub fn read_config<P: AsRef<Path>>(path: P) -> Result<RepConfig, ReError> {
    let mut file = File::open(path.as_ref())?;
    let mut s = String::new();
    let _ = file.read_to_string(&mut s);
    toml::from_str(s.as_str())
        .map_err(|e| ReError::ConfigFileParseErr(e.to_string()))
}

impl Config {
    pub fn create(core: RepConfig) -> Self {
        Self {
            core,
            max_memory: 0,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::config::read_config;
    use crate::err::CResult;

    #[test]
    fn test() -> CResult<()> {
        let c = read_config("../conf/replayer.toml");
        assert!(c.is_ok());
        Ok(())
    }
}
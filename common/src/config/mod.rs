mod load_style;

use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use crate::binlog::PAYLOAD_BUFFER_SIZE;
use crate::config::load_style::LoadStyle;

use crate::err::decode_error::ReError;

#[derive(Debug, Serialize, Deserialize)]
pub struct FConfig {
    config: RepConfig,

    /// 配置的加载方式
    load_style: LoadStyle,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepConfig {
    app_name: String,

    pub binlog: BinlogConfig,
    pub rc_mysql: RcMySQL,
    pub rc_metadata: RcMetadata,
    pub base: BaseConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseConfig {
    max_memory: Option<String>,

    /// 日志输出路径
    log_dir: Option<String>,
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

impl Default for FConfig {
    fn default() -> Self {
        FConfig {
            config: RepConfig::default(),
            load_style: LoadStyle::DEFAULT,
        }
    }
}

impl Default for RepConfig {
    fn default() -> Self {
        RepConfig {
            app_name: String::from(""),
            base: BaseConfig::default(),
            binlog: BinlogConfig::default(),
            rc_mysql: RcMySQL::default(),
            rc_metadata: RcMetadata::default(),
        }
    }
}

impl Default for RcMySQL {
    fn default() -> Self {
        RcMySQL {
            addr: vec![],
            username: "".to_string(),
            password: "".to_string(),
            raft_stats_fresh_interval_ms: None,
        }
    }
}

impl Default for RcMetadata {
    fn default() -> Self {
        RcMetadata {
            addr: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            database: "".to_string(),
            metadata_stats_fresh_interval_ms: None,
        }
    }
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

impl Default for BaseConfig {
    fn default() -> Self {
        BaseConfig {
            max_memory: None,
            log_dir: Some(String::from("/tmp/replayer")),
        }
    }
}

impl FConfig {
    pub fn new(c: RepConfig) -> Self {
        FConfig {
            config: c,
            load_style: LoadStyle::YAML,
        }
    }

    pub fn get_config(self) -> RepConfig {
        self.config
    }

    pub fn get_load_style(self) -> LoadStyle {
        self.load_style.clone()
    }
}

impl BaseConfig {
    pub fn get_log_dir(&self) -> Option<String> {
        self.log_dir.clone()
    }
}

/// 读取指定路径下的配制文件信息
pub fn read_config<P: AsRef<Path>>(path: P) -> Result<RepConfig, ReError> {
    let mut file = File::open(path.as_ref())?;
    let mut s = String::new();

    let _ = file.read_to_string(&mut s);
    toml::from_str(s.as_str())
        .map_err(|e| ReError::ConfigFileParseErr(e.to_string()))
}

#[cfg(test)]
mod test {
    use crate::config::read_config;
    use crate::err::CResult;

    #[test]
    fn test() -> CResult<()> {
        let c = read_config("../conf/replayer.toml");

        let rs =c.is_ok();
        assert!(rs);
        Ok(())
    }
}
pub mod load_style;

use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use crate::binlog::PAYLOAD_BUFFER_SIZE;
use crate::config::load_style::LoadStyle;

use crate::err::decode_error::ReError;

/// binlog 复制默认读超时(毫秒)。
///
/// 空闲时 socket 会周期性超时唤醒，使服务能及时响应停机信号；
/// 由于 `PacketChannel` 会保留半包数据，超时后重试是安全的。
pub const DEFAULT_BINLOG_READ_TIMEOUT_MILLIS: u64 = 1000;

/// binlog 复制默认主库心跳间隔(秒)。
///
/// 主库在空闲时按该间隔发送心跳事件，从库据此判断连接是否存活；
/// 设置为 0 表示禁用心跳(同时关闭空闲存活检测)。
pub const DEFAULT_BINLOG_HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// binlog 复制默认从库 server_id。
///
/// 必须非 0：MySQL 只有在 server_id 非 0 时才会把 COM_BINLOG_DUMP
/// 当作持续订阅(追平后继续等待新事件)，否则会在追平后发送 EOF 并断开。
pub const DEFAULT_BINLOG_SERVER_ID: u32 = 65535;

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
    host: Option<String>,
    port: Option<u16>,
    pub username: String,
    pub password: String,

    /// 读取binlog 的缓冲区大小
    pub payload_buffer_size: usize,

    /// binlog file, 如 mysql-bin.000005
    pub file: Option<String>,

    /// binlog file 消费的起始position
    pub position: Option<i64>,

    /// binlog 文件的绝对路径
    pub binlog_path: Option<String>,

    /// 单次 socket 读取超时(毫秒)。
    /// 空闲时会周期性超时唤醒，用于及时响应停机信号。
    /// `None` 或 0 表示不显式设置(沿用驱动推导值)。
    pub read_timeout_millis: Option<u64>,

    /// 主库心跳间隔(秒)，用于空闲时探活。
    /// 0 表示禁用心跳，同时也关闭空闲存活检测。
    pub heartbeat_interval_secs: Option<u64>,

    /// 从库 server_id，必须非 0 才能持续订阅 binlog。
    pub server_id: Option<u32>,

    /// SSL/TLS 配置，缺省表示不启用加密。
    pub ssl: Option<SslConfig>,
}

/// SSL/TLS 连接配置
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SslConfig {
    /// SSL 模式: DISABLED | IF_AVAILABLE | REQUIRED | VERIFY_CA | VERIFY_IDENTITY
    pub mode: Option<String>,

    /// CA 根证书路径(.pem/.der)
    pub root_cert_path: Option<String>,

    /// 客户端证书(pkcs12 格式)路径
    pub client_pkcs12_path: Option<String>,

    /// 客户端证书密码
    pub client_pkcs12_password: Option<String>,

    /// 是否跳过域名校验
    pub skip_domain_validation: Option<bool>,

    /// 是否接受无效证书
    pub accept_invalid_certs: Option<bool>,
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
            read_timeout_millis: Some(DEFAULT_BINLOG_READ_TIMEOUT_MILLIS),
            heartbeat_interval_secs: Some(DEFAULT_BINLOG_HEARTBEAT_INTERVAL_SECS),
            server_id: Some(DEFAULT_BINLOG_SERVER_ID),
            ssl: None,
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

impl BinlogConfig {
    pub fn get_host(&self) -> &str {
        match self.host.as_ref() {
            None => {
                ""
            }
            Some(s) => {
                &s
            }
        }
    }

    pub fn set_host(&mut self, host: Option<String>) {
        self.host = host
    }

    /// 是否已配置主机
    pub fn have_host(&self) -> bool {
        self.host.is_some()
    }

    pub fn get_port(&self) -> u16 {
        match self.port.as_ref() {
            None => {
                0
            }
            Some(p) => {
                p.clone()
            }
        }
    }

    pub fn set_port(&mut self, port: Option<u16>) {
        self.port = port
    }

    /// 是否已配置端口
    pub fn have_port(&self) -> bool {
        self.port.is_some()
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
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::Deserialize;

use crate::err::DecodeError::ReError;

#[derive(Debug, Deserialize)]
pub struct Config {
    binlog: BinlogConfig,
    rc_mysql: RcMySQL,
}

/// Binlog 配置
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BinlogConfig {
    /// binlog 文件的绝对路径
    pub binlog_path: String,

    /// binlog file, 如 mysql-bin.000005
    pub file: String,

    /// binlog file 消费的起始position
    pub position: i32,
}


#[derive(Debug, Deserialize)]
pub struct RcMySQL {
    addr: Vec<String>,
    username: String,
    password: String,
}

pub fn read_config<P: AsRef<Path>>(path: P) -> Result<Config, ReError> {
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
        assert!(c.is_ok());
        Ok(())
    }
}
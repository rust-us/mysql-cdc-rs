use std::fs;
use std::path::Path;

use connection::conn::connection::{Connection, IConnection};
use connection::conn::connection_options::ConnectionOptions;
use connection::conn::binlog_connection::{BinlogConnection, IBinlogConnection};
use connection::env_options::EnvOptions;
use common::log::tracing_factory::TracingFactory;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct TestConfig {
    mysql: MysqlConfig,
    test: TestSettings,
    binlog: BinlogConfig,
}

#[derive(Debug, Deserialize, Serialize)]
struct MysqlConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    #[serde(default = "default_database")]
    database: String,
    #[serde(default = "default_timeout")]
    timeout: u64,
    #[serde(default)]
    ssl: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct TestSettings {
    enabled: bool,
    #[serde(default)]
    verbose: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct BinlogConfig {
    enabled: bool,
    #[serde(default = "default_buffer_size")]
    buffer_size: usize,
}

fn default_database() -> String {
    "test".to_string()
}

fn default_timeout() -> u64 {
    30
}

fn default_buffer_size() -> usize {
    3072
}

/// 集成测试 - 需要真实的MySQL服务器
/// 
/// 运行这些测试需要：
/// 1. 复制 test-config.toml.example 为 test-config.toml
/// 2. 在 test-config.toml 中配置你的 MySQL 服务器信息
/// 3. 确保 MySQL 服务器正在运行并且可以连接
/// 
/// 运行方式：
/// ```bash
/// # 复制配置文件模板
/// cp connection/tests/test-config.toml.example connection/tests/test-config.toml
/// 
/// # 编辑配置文件，填入你的 MySQL 服务器信息
/// # 然后运行测试
/// cargo test --package connection --test integration_tests
/// ```
/// 

fn load_test_config() -> Option<TestConfig> {
    let config_path = Path::new("connection/tests/test-config.toml");
    if config_path.exists() {
        match fs::read_to_string(config_path) {
            Ok(content) => {
                match toml::from_str::<TestConfig>(&content) {
                    Ok(config) => {
                        println!("Loaded test configuration from: {}", config_path.display());
                        return Some(config);
                    }
                    Err(e) => {
                        println!("Failed to parse test config file: {}", e);
                        println!("Please check the format of {}", config_path.display());
                    }
                }
            }
            Err(e) => {
                println!("Failed to read test config file: {}", e);
            }
        }
    }
    None
}

fn should_run_integration_tests() -> (bool, Option<TestConfig>) {
    // 尝试加载配置文件
    if let Some(config) = load_test_config() {
        return (config.test.enabled, Some(config));
    }
    
    // 如果没有配置文件，则不运行集成测试
    (false, None)
}

fn get_test_connection_options() -> ConnectionOptions {
    let mut opts = ConnectionOptions::default();
    
    // 从配置文件加载
    if let Some(config) = load_test_config() {
        opts.hostname = config.mysql.host;
        opts.port = config.mysql.port as i16;
        opts.update_auth(config.mysql.username, config.mysql.password);
        return opts;
    }
    
    // 如果没有配置文件，返回默认配置（测试将被跳过）
    opts
}

fn should_run_binlog_tests() -> bool {
    if let Some(config) = load_test_config() {
        return config.binlog.enabled;
    }
    
    // 如果没有配置文件，默认启用 binlog 测试
    true
}

fn get_binlog_buffer_size() -> usize {
    if let Some(config) = load_test_config() {
        return config.binlog.buffer_size;
    }
    
    // 默认缓冲区大小
    3072
}

#[test]
fn test_mysql_connection() {
    let (should_run, config) = should_run_integration_tests();
    if !should_run {
        println!("Skipping integration test: MySQL integration tests disabled");
        println!("To enable: create connection/tests/test-config.toml with your MySQL server configuration");
        return;
    }
    
    if let Some(ref cfg) = config {
        if cfg.test.verbose {
            println!("Running MySQL connection test with config: {:?}", cfg.mysql);
        }
    }

    let opts = get_test_connection_options();
    let mut conn = Connection::new(opts);
    
    let channel_rs = conn.try_connect();
    assert!(channel_rs.is_ok(), "Failed to connect to MySQL server: {:?}", channel_rs.err());

    let query_result = conn.query(String::from("SELECT 1 + 1 as result"));
    assert!(query_result.is_ok(), "Failed to execute query: {:?}", query_result.err());
    
    let rows = query_result.unwrap();
    assert!(!rows.is_empty(), "Query should return at least one row");
    
    let values = &rows[0].as_slice();
    assert_eq!(values[0].clone().unwrap(), "2", "Query result should be '2'");
}

#[test]
fn test_mysql_binlog_connection() {
    let (should_run, config) = should_run_integration_tests();
    if !should_run {
        println!("Skipping integration test: MySQL integration tests disabled");
        return;
    }
    
    if let Some(ref cfg) = config {
        if cfg.test.verbose {
            println!("Running MySQL binlog connection test with config: {:?}", cfg.mysql);
        }
    }

    let opts = get_test_connection_options();
    let mut binlog_conn = BinlogConnection::new(&opts);
    
    let channel_rs = binlog_conn.try_connect();
    assert!(channel_rs.is_ok(), "Failed to connect to MySQL server for binlog: {:?}", channel_rs.err());

    let query_result = binlog_conn.query(String::from("SELECT 1 + 1 as result"));
    assert!(query_result.is_ok(), "Failed to execute query on binlog connection: {:?}", query_result.err());
    
    let rows = query_result.unwrap();
    assert!(!rows.is_empty(), "Query should return at least one row");
    
    let values = &rows[0].as_slice();
    assert_eq!(values[0].clone().unwrap(), "2", "Query result should be '2'");
}

#[test]
fn test_mysql_binlog_events() {
    let (should_run, config) = should_run_integration_tests();
    if !should_run {
        println!("Skipping integration test: MySQL integration tests disabled");
        return;
    }
    
    if !should_run_binlog_tests() {
        println!("Skipping binlog test: binlog tests disabled in configuration");
        return;
    }

    let verbose = config.as_ref().map(|c| c.test.verbose).unwrap_or(false);
    if verbose {
        TracingFactory::init_log(true);
    }

    let mut opts = get_test_connection_options();
    if verbose {
        opts.set_env(EnvOptions::debug());
    }
    
    let mut binlog_conn = BinlogConnection::new(&opts);
    
    // 注意：这个测试可能需要特定的binlog配置
    // 如果MySQL服务器没有启用binlog或者没有适当的权限，这个测试可能会失败
    let buffer_size = get_binlog_buffer_size();
    let binlog_event_rs = binlog_conn.binlog(buffer_size);
    
    if binlog_event_rs.is_err() {
        println!("Binlog test skipped - this is expected if binlog is not enabled or user lacks REPLICATION privileges");
        println!("Error: {:?}", binlog_event_rs.err());
        return;
    }

    let mut binlog_event = binlog_event_rs.unwrap();

    // 尝试读取一些binlog事件
    let mut event_count = 0;
    for x in binlog_event.get_iter() {
        if x.is_ok() {
            let list = x.unwrap();
            assert!(list.len() > 0, "Binlog events list should not be empty");
            
            if verbose || (opts.env.is_some() && opts.env.as_ref().unwrap().borrow().is_debug()) {
                println!("Received {} binlog events", list.len());
            }
            
            event_count += list.len();
            
            // 只读取少量事件进行测试
            if event_count >= 10 {
                break;
            }
        } else {
            println!("Error reading binlog events: {:?}", x.err());
            break;
        }
    }
    
    println!("Successfully read {} binlog events", event_count);
}

#[test]
fn test_binlog_connection_basic() {
    let (should_run, config) = should_run_integration_tests();
    if !should_run {
        println!("Skipping integration test: MySQL integration tests disabled");
        println!("To enable: create connection/tests/test-config.toml with your MySQL server configuration");
        return;
    }
    
    if let Some(ref cfg) = config {
        if cfg.test.verbose {
            println!("Running binlog connection basic test with config: {:?}", cfg.mysql);
        }
    }

    let opts = get_test_connection_options();
    let mut binlog_conn = BinlogConnection::new(&opts);
    
    let channel_rs = binlog_conn.try_connect();
    assert!(channel_rs.is_ok(), "Failed to connect to MySQL server for binlog: {:?}", channel_rs.err());

    let query = binlog_conn.query(String::from("SELECT 1 + 1")).expect("binlog connection query error");
    let values = &query[0].as_slice();
    assert_eq!(values[0].clone().unwrap(), "2", "Query result should be '2'");
}

#[test]
fn test_binlog_events_detailed() {
    let (should_run, config) = should_run_integration_tests();
    if !should_run {
        println!("Skipping integration test: MySQL integration tests disabled");
        return;
    }
    
    if !should_run_binlog_tests() {
        println!("Skipping binlog test: binlog tests disabled in configuration");
        return;
    }

    let verbose = config.as_ref().map(|c| c.test.verbose).unwrap_or(false);
    if verbose {
        TracingFactory::init_log(true);
    }

    let mut opts = get_test_connection_options();
    if verbose {
        opts.set_env(EnvOptions::debug());
    }

    let mut binlog_conn = BinlogConnection::new(&opts);
    let buffer_size = get_binlog_buffer_size();
    let binlog_event_rs = binlog_conn.binlog(buffer_size);
    
    if binlog_event_rs.is_err() {
        println!("Binlog detailed test skipped - this is expected if binlog is not enabled or user lacks REPLICATION privileges");
        println!("Error: {:?}", binlog_event_rs.err());
        return;
    }

    let mut binlog_event = binlog_event_rs.unwrap();

    for x in binlog_event.get_iter() {
        if x.is_ok() {
            let list = x.unwrap();

            assert!(list.len() > 0, "Binlog events list should not be empty");
            if verbose || (opts.env.is_some() && opts.env.as_ref().unwrap().borrow().is_debug()) {
                println!("Received binlog events: {:?}", list);

                for e in list {
                    use binlog::events::binlog_event::BinlogEvent;
                    
                    let event_type = BinlogEvent::get_type_name(&e);
                    // Note: log_context is private, so we can't access it directly in tests
                    // This is acceptable for integration tests
                    println!("event: {:?}", event_type);
                }
            }
            
            // 只处理第一批事件进行测试
            break;
        } else {
            println!("Error reading binlog events: {:?}", x.err());
            break;
        }
    }
}

#[test]
fn test_connection_error_handling() {
    // 测试连接到不存在的服务器
    let mut opts = ConnectionOptions::default();
    opts.hostname = "nonexistent.host".to_string();
    opts.port = 9999;
    opts.update_auth("test".to_string(), "test".to_string());
    
    let mut conn = Connection::new(opts);
    let channel_rs = conn.try_connect();
    
    assert!(channel_rs.is_err(), "Connection to nonexistent host should fail");
    
    // 验证错误类型
    match channel_rs.err().unwrap() {
        common::err::decode_error::ReError::IoError(_) => {
            // 这是预期的错误类型
        }
        other => {
            panic!("Unexpected error type: {:?}", other);
        }
    }
}



#[cfg(test)]
mod config_tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_config_parsing() {
        // 测试配置文件解析功能
        let sample_config = r#"
[mysql]
host = "test.example.com"
port = 3307
username = "testuser"
password = "testpass"
database = "testdb"
timeout = 60
ssl = true

[test]
enabled = true
verbose = false

[binlog]
enabled = true
buffer_size = 2048
"#;

        let config: TestConfig = toml::from_str(sample_config).expect("Failed to parse config");
        
        assert_eq!(config.mysql.host, "test.example.com");
        assert_eq!(config.mysql.port, 3307);
        assert_eq!(config.mysql.username, "testuser");
        assert_eq!(config.mysql.password, "testpass");
        assert_eq!(config.mysql.database, "testdb");
        assert_eq!(config.mysql.timeout, 60);
        assert_eq!(config.mysql.ssl, true);
        
        assert_eq!(config.test.enabled, true);
        assert_eq!(config.test.verbose, false);
        
        assert_eq!(config.binlog.enabled, true);
        assert_eq!(config.binlog.buffer_size, 2048);
    }

    #[test]
    fn test_config_defaults() {
        // 测试默认值
        let minimal_config = r#"
[mysql]
host = "localhost"
port = 3306
username = "root"
password = "password"

[test]
enabled = false

[binlog]
enabled = false
"#;

        let config: TestConfig = toml::from_str(minimal_config).expect("Failed to parse minimal config");
        
        // 测试默认值
        assert_eq!(config.mysql.database, "test"); // 默认数据库
        assert_eq!(config.mysql.timeout, 30); // 默认超时
        assert_eq!(config.mysql.ssl, false); // 默认不使用 SSL
        assert_eq!(config.test.verbose, false); // 默认不详细输出
        assert_eq!(config.binlog.buffer_size, 3072); // 默认缓冲区大小
    }

    #[test]
    fn test_sample_config_file() {
        // 测试示例配置文件是否能正确解析
        let sample_path = "connection/tests/test-config-sample.toml";
        if Path::new(sample_path).exists() {
            let content = fs::read_to_string(sample_path).expect("Failed to read sample config");
            let config: TestConfig = toml::from_str(&content).expect("Failed to parse sample config file");
            
            assert_eq!(config.mysql.host, "example.com");
            assert_eq!(config.test.enabled, false); // 示例配置应该禁用测试
            assert_eq!(config.binlog.enabled, false); // 示例配置应该禁用 binlog 测试
        }
    }
}
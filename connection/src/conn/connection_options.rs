use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use native_tls::Identity;

use common::err::decode_error::ReError;
use common::err::CResult;

use crate::binlog::binlog_options::{BinlogOptions, BinlogOptionsRef};
use crate::conn::ssl_mode::SslMode;
use crate::env_options::{EnvOptions, EnvOptionsRef};

pub type ConnectionOptionsRef = Arc<RefCell<ConnectionOptions>>;

/// Settings used to connect to MySQL/MariaDB.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// Port number to connect. Defaults to 3306.
    pub port: i16,

    /// Hostname to connect. Defaults to "localhost".
    pub hostname: String,

    /// Defines whether SSL/TLS must be used. Defaults to SslMode.DISABLED.
    pub ssl_mode: SslMode,

    /// A database user which is used to register as a database slave.
    /// The user needs to have <c>REPLICATION SLAVE</c>, <c>REPLICATION CLIENT</c> privileges.
    pub username: String,

    /// The password of the user which is used to connect.
    pub password: String,

    /// Default database name specified in Handshake connection.
    /// Has nothing to do with filtering events by database name.
    pub database: Option<String>,

    /// Specifies the slave server id and used only in blocking mode. Defaults to 65535.
    /// <a href="https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-server-id.html">See more</a>
    pub server_id: u32,

    /// Specifies whether to stream events or read until last event and then return.
    /// Defaults to true (stream events and wait for new ones).
    pub blocking: bool,

    /// Defines interval of keep alive messages that the master sends to the slave.
    /// Defaults to 30 seconds.
    pub heartbeat_interval: Duration,

    /// Defines the binlog coordinates that replication should start from.
    /// Defaults to BinlogOptions.FromEnd()
    pub binlog: Option<BinlogOptionsRef>,

    pub env: Option<EnvOptionsRef>,

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    pub ssl_opts: Option<SslOpts>,
}

impl Default for ConnectionOptions {
    fn default() -> ConnectionOptions {
        ConnectionOptions {
            port: 3306,
            hostname: String::from("localhost"),
            ssl_mode: SslMode::Disabled,
            username: String::new(),
            password: String::new(),
            database: None,
            server_id: 65535,
            blocking: true,
            heartbeat_interval: Duration::from_secs(30),
            binlog: Some(Arc::new(RefCell::new(BinlogOptions::from_start()))),
            env: Some(Arc::new(RefCell::new(EnvOptions::default()))),
            ssl_opts: None,
        }
    }
}

impl ConnectionOptions {
    pub fn new_str(hostname: &str, port: i16, username: &str, password: &str) -> ConnectionOptions {
        ConnectionOptions::new(
            hostname.to_string(),
            port,
            username.to_string(),
            password.to_string(),
        )
    }

    pub fn new(
        hostname: String,
        port: i16,
        username: String,
        password: String,
    ) -> ConnectionOptions {
        ConnectionOptions::new_with_binlog(
            hostname,
            port,
            username,
            password,
            BinlogOptions::from_start(),
        )
    }

    pub fn new_with_binlog(
        hostname: String,
        port: i16,
        username: String,
        password: String,
        binlog: BinlogOptions,
    ) -> ConnectionOptions {
        ConnectionOptions {
            hostname: hostname.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            database: None,
            ssl_mode: SslMode::Disabled,
            server_id: 0,
            blocking: false,
            heartbeat_interval: Duration::default(),
            binlog: Some(Arc::new(RefCell::new(binlog))),
            env: None,
            ssl_opts: None,
        }
    }

    pub fn update_server_id(&mut self, server_id: u32) {
        self.server_id = server_id;
    }

    pub fn update_auth(&mut self, username: String, password: String) {
        self.username = username;
        self.password = password;
    }

    pub fn update_binlog_position(&mut self, filename: String, pos: u64) {
        if self.binlog.is_some() {
            self.binlog.as_mut().unwrap().borrow_mut().filename = filename;
            self.binlog
                .as_mut()
                .unwrap()
                .borrow_mut()
                .update_position(pos);
        }
    }

    pub fn set_env(&mut self, env: EnvOptions) {
        self.env = Some(Arc::new(RefCell::new(env)));
    }

    pub fn set_env_ref(&mut self, env: EnvOptionsRef) {
        self.env = Some(env);
    }

    pub fn is_debug(&self) -> bool {
        match self.env.as_ref() {
            None => false,
            Some(env) => env.borrow().is_debug(),
        }
    }
}

/// Ssl 配置.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct SslOpts {
    client_identity: Option<ClientIdentity>,
    root_cert_path: Option<String>,
    skip_domain_validation: bool,
    accept_invalid_certs: bool,
}

/// SSL配置属性
impl SslOpts {
    /// 设置 client identity.
    pub fn with_client_identity(mut self, identity: Option<ClientIdentity>) -> Self {
        self.client_identity = identity;
        self
    }

    /// 设置证书路径
    ///
    /// 支持证书格式 .der .pem.
    /// ,pem证书中允许多个证书
    pub fn with_root_cert_path(mut self, root_cert_path: Option<String>) -> Self {
        self.root_cert_path = root_cert_path;
        self
    }

    /// 不验证服务器域
    /// (defaults to `false`).
    pub fn with_danger_skip_domain_validation(mut self, value: bool) -> Self {
        self.skip_domain_validation = value;
        self
    }

    /// true时接受无效证书
    /// (defaults to `false`).
    pub fn with_danger_accept_invalid_certs(mut self, value: bool) -> Self {
        self.accept_invalid_certs = value;
        self
    }

    pub fn client_identity(&self) -> Option<&ClientIdentity> {
        self.client_identity.as_ref()
    }

    pub fn root_cert_path(&self) -> Option<&Path> {
        self.root_cert_path.as_ref().map(Path::new)
    }

    pub fn skip_domain_validation(&self) -> bool {
        self.skip_domain_validation
    }

    pub fn accept_invalid_certs(&self) -> bool {
        self.accept_invalid_certs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientIdentity {
    pkcs12_path: String,
    password: Option<String>,
}

impl ClientIdentity {
    /// Creates new identity with the given path to the pkcs12 archive.
    pub fn new<T>(pkcs12_path: String) -> Self {
        Self {
            pkcs12_path,
            password: None,
        }
    }

    /// Sets the archive password.
    pub fn with_password<T>(mut self, pass: String) -> Self {
        self.password = Some(pass);
        self
    }

    /// Returns the pkcs12 archive path.
    pub fn pkcs12_path(&self) -> &Path {
        Path::new(&self.pkcs12_path)
    }

    /// Returns the archive password.
    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(AsRef::as_ref)
    }

    pub(crate) fn load(&self) -> CResult<Identity> {
        let der = std::fs::read(&self.pkcs12_path)?;
        match Identity::from_pkcs12(&der, self.password.as_deref().unwrap_or("")) {
            Ok(identity) => Ok(identity),
            Err(err) => Err(ReError::ConnectionError(format!(
                "Can not load identity. err:{{{err}}}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::conn::connection_options::ConnectionOptions;
    use crate::env_options::EnvOptions;

    #[test]
    fn test() {
        let mut opts = ConnectionOptions::default();
        assert!(!opts.is_debug());

        opts.set_env(EnvOptions::debug());
        assert!(opts.is_debug());
        assert!(opts.is_debug());
        assert!(opts.is_debug());
    }
}

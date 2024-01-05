use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};
use std::process;

use bytes::Buf;
use flate2::Compression;
use mysql_common::constants::{
    CapabilityFlags, StatusFlags, DEFAULT_MAX_ALLOWED_PACKET, MAX_PAYLOAD_LEN, UTF8MB4_GENERAL_CI,
    UTF8_GENERAL_CI,
};
use mysql_common::crypto;
use mysql_common::io::{ParseBuf, ReadMysqlExt};
use mysql_common::packets::binlog_request::BinlogRequest;
use mysql_common::packets::{
    AuthPlugin, AuthSwitchRequest, Column, CommonOkPacket, ErrPacket, HandshakePacket,
    HandshakeResponse, OkPacket, OkPacketDeserializer, OkPacketKind, OldAuthSwitchRequest,
    OldEofPacket, ResultSetTerminator, SslRequest,
};
use mysql_common::proto::sync_framed::MySyncFramed;
use mysql_common::proto::MySerialize;
use mysql_common::value::convert::from_value_opt;
use mysql_common::Value::NULL;

use crate::mysql::buffer_pool::get_buffer;
use crate::mysql::buffer_pool::Buffer;
use crate::mysql::conn::opts::Opts;
use crate::mysql::conn::opts::SslOpts;
use crate::mysql::error::DriverError;
use crate::mysql::error::Error;
/// Mysql connection.
use crate::mysql::error::Result;
use crate::mysql::handler::query::query_handler;
use crate::mysql::handler::query::query_result::Or;
use crate::mysql::io::Stream;

/// Mutable connection.
#[derive(Debug)]
pub enum ConnMut<'c> {
    Mut(&'c mut Connection),
    Owned(Connection),
}

impl From<Connection> for ConnMut<'static> {
    fn from(conn: Connection) -> Self {
        ConnMut::Owned(conn)
    }
}

impl<'a> From<&'a mut Connection> for ConnMut<'a> {
    fn from(conn: &'a mut Connection) -> Self {
        ConnMut::Mut(conn)
    }
}

impl Deref for ConnMut<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match self {
            ConnMut::Mut(conn) => conn,
            ConnMut::Owned(conn) => conn,
        }
    }
}

impl DerefMut for ConnMut<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        match self {
            ConnMut::Mut(conn) => conn,
            ConnMut::Owned(ref mut conn) => conn,
        }
    }
}

/// Connection 内部属性.
#[derive(Debug)]
struct ConnInner {
    opts: Opts,
    // mysql协议的同步流
    stream: Option<MySyncFramed<Stream>>,
    //stmt_cache: StmtCache,
    server_version: Option<(u16, u16, u16)>,

    /// Last Ok packet, if any.
    ok_packet: Option<OkPacket<'static>>,
    capability_flags: CapabilityFlags,
    connection_id: u32,
    status_flags: StatusFlags,
    character_set: u8,
    last_command: u8,
    connected: bool,
    has_results: bool,
    //local_infile_handler: Option<LocalInfileHandler>,
    auth_plugin: AuthPlugin<'static>,
    // 数据库认证时使用的随机数，握手时从服务端获取
    nonce: Vec<u8>,
}

impl ConnInner {
    fn empty(opts: Opts) -> Self {
        ConnInner {
            //stmt_cache: StmtCache::new(opts.get_stmt_cache_size()),
            stream: None,
            capability_flags: CapabilityFlags::empty(),
            status_flags: StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            ok_packet: None,
            last_command: 0u8,
            connected: false,
            has_results: false,
            server_version: None,
            // local_infile_handler: None,
            auth_plugin: AuthPlugin::MysqlNativePassword,
            nonce: Vec::new(),
            opts,
        }
    }
}

#[derive(Debug)]
pub struct Connection(Box<ConnInner>);

impl Connection {
    pub fn has_capability(&self, flag: CapabilityFlags) -> bool {
        self.0.capability_flags.contains(flag)
    }

    /// 获得能力flag的拷贝
    pub fn get_copy_capability_flags(&self) -> CapabilityFlags {
        // CapabilityFlags已设置属性为copy
        self.0.capability_flags
    }

    /// 返回连接id
    pub fn connection_id(&self) -> u32 {
        self.0.connection_id
    }

    fn stream_ref(&self) -> &MySyncFramed<Stream> {
        self.0.stream.as_ref().expect("incomplete connection")
    }

    /// 获取可变的mysql流
    fn stream_mut(&mut self) -> &mut MySyncFramed<Stream> {
        self.0.stream.as_mut().expect("incomplete connection")
    }

    fn is_insecure(&self) -> bool {
        self.stream_ref().get_ref().is_insecure()
    }

    fn is_socket(&self) -> bool {
        self.stream_ref().get_ref().is_socket()
    }

    /// 主动关闭连接
    /// drop时steam会自动关闭，可以不主动调用
    pub fn shutdown(&mut self) -> Result<()> {
        self.stream_mut().get_mut().shutdown()
    }

    /// 创建一个新的mysql连接
    pub fn new<T, E>(opts: T) -> Result<Connection>
    where
        Opts: TryFrom<T, Error = E>,
        Error: From<E>,
    {
        let opts = Opts::try_from(opts)?;
        let mut conn = Connection(Box::new(ConnInner::empty(opts)));
        conn.connect_stream()?;
        conn.connect()?;
        // 存在初始化sql时执行
        for cmd in conn.0.opts.get_init() {
            query_handler::query_drop(&mut conn, cmd)?;
        }
        Ok(conn)
    }

    /// 根据已有连接属性创建一个新的mysql连接
    pub fn fork(ori: &Connection) -> Result<Connection> {
        // 复制原连接的配置
        let opts = ori.0.opts.clone();
        Connection::new(opts)
    }

    /// 发送指定的mysql command
    pub fn write_command<T: MySerialize>(&mut self, cmd: &T) -> Result<()> {
        let mut buf = get_buffer();
        cmd.serialize(buf.as_mut());
        self.reset_seq_id();
        debug_assert!(buf.len() > 0);
        self.0.last_command = buf[0];
        self.write_packet(&mut &*buf)
    }

    /// 处理结果包result set
    pub fn handle_result_set(&mut self) -> Result<Or<Vec<Column>, OkPacket<'static>>> {
        if self.more_results_exists() {
            self.sync_seq_id();
        }

        let pld = self.read_packet()?;
        match pld[0] {
            0x00 => {
                let ok = self.handle_ok::<CommonOkPacket>(&pld)?;
                Ok(Or::B(ok.into_owned()))
            }
            // TODO 0xfb时需要处理本地文件local_infile_handler
            // 其他情况处理返回的result set
            _ => {
                let mut reader = &pld[..];
                let column_count = reader.read_lenenc_int()?;
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for _ in 0..column_count {
                    let pld = self.read_packet()?;
                    let column = ParseBuf(&pld).parse(())?;
                    columns.push(column);
                }
                // 丢弃 eof packet
                self.drop_packet()?;
                self.0.has_results = column_count > 0;
                Ok(Or::A(columns))
            }
        }
    }

    // 注册slave
    fn register_as_slave(&mut self, server_id: u32) -> Result<()> {
        use mysql_common::packets::ComRegisterSlave;

        // set @master_binlog_checksum= @@global.binlog_checksum
        //query_handler::query_drop(self, "SET @master_binlog_checksum='ALL'")?;
        self.write_command(&ComRegisterSlave::new(server_id))?;

        // Server will respond with OK.
        self.read_packet()?;

        Ok(())
    }

    /// 请求binlog数据
    pub fn request_binlog(&mut self, request: BinlogRequest<'_>) -> Result<()> {
        self.register_as_slave(request.server_id())?;
        self.write_command(&request.as_cmd())?;
        Ok(())
    }

    /// 是否有更多的result, 处理MultiResultSet时使用
    pub fn more_results_exists(&self) -> bool {
        self.0
            .status_flags
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    /// 同步当前的seq id，用于继续处理
    fn sync_seq_id(&mut self) {
        self.stream_mut().codec_mut().sync_seq_id();
    }

    /// 将当前连接切换到ssl
    fn switch_to_ssl(&mut self, ssl_opts: SslOpts) -> Result<()> {
        let stream = self.0.stream.take().expect("incomplete conn");
        let (in_buf, out_buf, codec, stream) = stream.destruct();
        let stream = stream.make_secure(self.0.opts.get_host(), ssl_opts)?;
        let stream = MySyncFramed::construct(in_buf, out_buf, codec, stream);
        self.0.stream = Some(stream);
        Ok(())
    }

    /// 建立tcp连接
    fn connect_stream(&mut self) -> Result<()> {
        let opts = &self.0.opts;
        let read_timeout = opts.get_read_timeout().cloned();
        let write_timeout = opts.get_write_timeout().cloned();
        let tcp_keepalive_time = opts.get_tcp_keepalive_time_ms();
        let tcp_keepalive_probe_interval_secs = opts.get_tcp_keepalive_probe_interval_secs();
        let tcp_keepalive_probe_count = opts.get_tcp_keepalive_probe_count();
        let tcp_user_timeout = opts.get_tcp_user_timeout_ms();
        let tcp_nodelay = opts.get_tcp_nodelay();
        let tcp_connect_timeout = opts.get_tcp_connect_timeout();
        let bind_address = opts.bind_address().cloned();
        let stream = if let Some(socket) = opts.get_socket() {
            Stream::connect_socket(socket, read_timeout, write_timeout)?
        } else {
            let port = opts.get_tcp_port();
            let ip_or_hostname = match opts.get_host() {
                url::Host::Domain(domain) => domain,
                url::Host::Ipv4(ip) => ip.to_string(),
                url::Host::Ipv6(ip) => ip.to_string(),
            };
            Stream::connect_tcp(
                &ip_or_hostname,
                port,
                read_timeout,
                write_timeout,
                tcp_keepalive_time,
                tcp_keepalive_probe_interval_secs,
                tcp_keepalive_probe_count,
                tcp_user_timeout,
                tcp_nodelay,
                tcp_connect_timeout,
                bind_address,
            )?
        };
        self.0.stream = Some(MySyncFramed::new(stream));
        Ok(())
    }

    /// 和mysql服务端进行mysql协议握手，建立通信
    fn connect(&mut self) -> Result<()> {
        if self.0.connected {
            return Ok(());
        }
        self.do_handshake()
            .and_then(|_| {
                Ok(from_value_opt::<usize>(
                    query_handler::query_system_var(self, "max_allowed_packet")?.unwrap_or(NULL),
                )
                .unwrap_or(0))
            })
            .and_then(|max_allowed_packet| {
                if max_allowed_packet == 0 {
                    Err(Error::DriverError(DriverError::SetupError))
                } else {
                    self.stream_mut().codec_mut().max_allowed_packet = max_allowed_packet;
                    self.0.connected = true;
                    Ok(())
                }
            })
    }

    /// 进行mysql握手
    fn do_handshake(&mut self) -> Result<()> {
        // 获取server发送的第一个握手包
        let payload = self.read_packet()?;
        let handshake = ParseBuf(&payload).parse::<HandshakePacket>(())?;

        // 协议版本号0x0A/10
        if handshake.protocol_version() != 10u8 {
            return Err(Error::DriverError(DriverError::UnsupportedProtocol(
                handshake.protocol_version(),
            )));
        }

        // 必须是CLIENT_PROTOCOL_41
        if !handshake
            .capabilities()
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            return Err(Error::DriverError(DriverError::Protocol41NotSet));
        }

        // 记录握手包信息
        self.handle_handshake(&handshake);

        // 如果是ssl连接，发送ssl包
        if self.is_insecure() {
            if let Some(ssl_opts) = self.0.opts.get_ssl_opts().cloned() {
                if !self.has_capability(CapabilityFlags::CLIENT_SSL) {
                    return Err(Error::DriverError(DriverError::TlsNotSupported));
                } else {
                    self.do_ssl_request()?;
                    self.switch_to_ssl(ssl_opts)?;
                }
            }
        }

        // 记录握手时获取的随机数，用于认证，长度始终为21(20位+1位填充)
        self.0.nonce = {
            let mut nonce = Vec::from(handshake.scramble_1_ref());
            nonce.extend_from_slice(handshake.scramble_2_ref().unwrap_or(&[][..]));
            // 去除末尾的1位填充位
            nonce.resize(20, 0);
            nonce
        };

        // 只支持 CachingSha2Password 和 MysqlNativePassword 两种方式
        self.0.auth_plugin = match handshake.auth_plugin() {
            Some(x @ AuthPlugin::CachingSha2Password) => x.into_owned(),
            _ => AuthPlugin::MysqlNativePassword,
        };

        // 发送握手结果并方完成认证
        self.write_handshake_response()?;
        self.continue_auth(false)?;

        // 需要客户端压缩的情况开启压缩
        if self.has_capability(CapabilityFlags::CLIENT_COMPRESS) {
            self.switch_to_compressed();
        }

        Ok(())
    }

    /// 从流中读取一个mysql packet，此时不关心packet内容
    fn raw_read_packet(&mut self, buffer: &mut Vec<u8>) -> Result<()> {
        if !self.stream_mut().next_packet(buffer)? {
            // 读取失败表示连接已经断开
            Err(Error::server_disconnected())
        } else {
            Ok(())
        }
    }

    /// 读取一个mysql packet，并处理err包
    pub fn read_packet(&mut self) -> Result<Buffer> {
        loop {
            let mut buffer = get_buffer();
            match self.raw_read_packet(buffer.as_mut()) {
                // 处理获取的packet header为FF，即ERR包的情况
                Ok(()) if buffer.first() == Some(&0xff) => {
                    //let tem0 = ParseBuf(&buffer).parse(self.0.capability_flags)?;
                    //print!("c{}", tem0);
                    // ErrPacket需要capability_flags参数用于解析
                    match ParseBuf(&buffer).parse(self.0.capability_flags)? {
                        ErrPacket::Error(server_error) => {
                            self.handle_err();
                            return Err(Error::MySqlError(From::from(server_error)));
                        }
                        ErrPacket::Progress(_progress_report) => {
                            //非error包的情况直接继续
                            // mysql-common的枚举类型，mysql协议中未使用
                            continue;
                        }
                    }
                }
                Ok(()) => return Ok(buffer),
                Err(e) => {
                    self.handle_err();
                    return Err(e);
                }
            }
        }
    }

    /// 读取下一个row包
    pub fn next_row_packet(&mut self) -> Result<Option<Buffer>> {
        if !self.0.has_results {
            return Ok(None);
        }

        let pld = self.read_packet()?;

        // 处理EOF包
        if self.has_capability(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
            if pld[0] == 0xfe && pld.len() < MAX_PAYLOAD_LEN {
                self.0.has_results = false;
                self.handle_ok::<ResultSetTerminator>(&pld)?;
                return Ok(None);
            }
        } else if pld[0] == 0xfe && pld.len() < 8 {
            self.0.has_results = false;
            self.handle_ok::<OldEofPacket>(&pld)?;
            return Ok(None);
        }

        Ok(Some(pld))
    }

    /// 连接发生异常，清空部分连接数据
    fn handle_err(&mut self) {
        self.0.status_flags = StatusFlags::empty();
        self.0.has_results = false;
        self.0.ok_packet = None;
    }

    /// 记录握手包中获得的server信息
    fn handle_handshake(&mut self, hp: &HandshakePacket<'_>) {
        self.0.capability_flags = hp.capabilities() & self.get_client_flags();
        self.0.status_flags = hp.status_flags();
        self.0.connection_id = hp.connection_id();
        self.0.character_set = hp.default_collation();
        self.0.server_version = hp.server_version_parsed();
    }

    /// 发送ssl的握手包
    fn do_ssl_request(&mut self) -> Result<()> {
        let charset = if self.0.server_version.unwrap() >= (5, 5, 3) {
            UTF8MB4_GENERAL_CI
        } else {
            UTF8_GENERAL_CI
        };

        let ssl_request = SslRequest::new(
            self.get_client_flags(),
            DEFAULT_MAX_ALLOWED_PACKET as u32,
            charset as u8,
        );
        self.write_struct(&ssl_request)
    }

    /// 重置mysql command的seq id
    fn reset_seq_id(&mut self) {
        self.stream_mut().codec_mut().reset_seq_id();
    }

    /// 将struct序列化为packet包buffer后写出
    fn write_struct<T: MySerialize>(&mut self, s: &T) -> Result<()> {
        let mut buf = get_buffer();
        s.serialize(buf.as_mut());
        self.write_packet(&mut &*buf)
    }

    /// 写出packet包Buffer
    fn write_packet<T: Buf>(&mut self, data: &mut T) -> Result<()> {
        self.stream_mut().send(data)?;
        Ok(())
    }

    /// 获得client能力flag
    fn get_client_flags(&self) -> CapabilityFlags {
        let mut client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_LOCAL_FILES
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | (self.0.capability_flags & CapabilityFlags::CLIENT_LONG_FLAG);
        if self.0.opts.get_compress().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_COMPRESS);
        }
        if self.0.opts.get_connect_attrs().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_CONNECT_ATTRS);
        }
        if let Some(db_name) = self.0.opts.get_db_name() {
            if !db_name.is_empty() {
                client_flags.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
            }
        }
        if self.is_insecure() && self.0.opts.get_ssl_opts().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_SSL);
        }
        client_flags | self.0.opts.get_additional_capabilities()
    }

    /// 启用客户端压缩
    fn switch_to_compressed(&mut self) {
        self.stream_mut()
            .codec_mut()
            .compress(Compression::default());
    }

    /// 发送握手响应包
    fn write_handshake_response(&mut self) -> Result<()> {
        let auth_data = self
            .0
            .auth_plugin
            .gen_data(self.0.opts.get_pass(), &self.0.nonce)
            .map(|x| x.into_owned());

        let handshake_response = HandshakeResponse::new(
            auth_data.as_deref(),
            self.0.server_version.unwrap_or((0, 0, 0)),
            self.0.opts.get_user().map(str::as_bytes),
            self.0.opts.get_db_name().map(str::as_bytes),
            Some(self.0.auth_plugin.clone()),
            self.0.capability_flags,
            self.connect_attrs(),
        );

        let mut buf = get_buffer();
        handshake_response.serialize(buf.as_mut());
        self.write_packet(&mut &*buf)
    }

    /// 按照认证类型处理，完成认证
    fn continue_auth(&mut self, auth_switched: bool) -> Result<()> {
        match self.0.auth_plugin {
            AuthPlugin::CachingSha2Password => {
                self.continue_caching_sha2_password_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::MysqlNativePassword | AuthPlugin::MysqlOldPassword => {
                self.continue_mysql_native_password_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::MysqlClearPassword => {
                if !self.0.opts.get_enable_cleartext_plugin() {
                    return Err(Error::DriverError(DriverError::CleartextPluginDisabled));
                }
                self.continue_mysql_native_password_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::Other(ref name) => {
                let plugin_name = String::from_utf8_lossy(name).into();
                Err(Error::DriverError(DriverError::UnknownAuthPlugin(
                    plugin_name,
                )))
            }
        }
    }

    /// 处理CachingSha2Password
    fn continue_caching_sha2_password_auth(&mut self, auth_switched: bool) -> Result<()> {
        let payload = self.read_packet()?;

        match payload[0] {
            0x00 => {
                // ok packet for empty password
                Ok(())
            }
            0x01 => match payload[1] {
                0x03 => {
                    let payload = self.read_packet()?;
                    self.handle_ok::<CommonOkPacket>(&payload).map(drop)
                }
                0x04 => {
                    if !self.is_insecure() || self.is_socket() {
                        let mut pass = self
                            .0
                            .opts
                            .get_pass()
                            .map(Vec::from)
                            .unwrap_or_else(Vec::new);
                        pass.push(0);
                        self.write_packet(&mut pass.as_slice())?;
                    } else {
                        self.write_packet(&mut &[0x02][..])?;
                        let payload = self.read_packet()?;
                        let key = &payload[1..];
                        let mut pass = self
                            .0
                            .opts
                            .get_pass()
                            .map(Vec::from)
                            .unwrap_or_else(Vec::new);
                        pass.push(0);
                        for (i, c) in pass.iter_mut().enumerate() {
                            *(c) ^= self.0.nonce[i % self.0.nonce.len()];
                        }
                        let encrypted_pass = crypto::encrypt(&pass, key);
                        self.write_packet(&mut encrypted_pass.as_slice())?;
                    }

                    let payload = self.read_packet()?;
                    self.handle_ok::<CommonOkPacket>(&payload).map(drop)
                }
                _ => Err(Error::DriverError(DriverError::UnexpectedPacket)),
            },
            0xfe if !auth_switched => {
                let auth_switch_request = ParseBuf(&payload).parse(())?;
                self.perform_auth_switch(auth_switch_request)
            }
            _ => Err(Error::DriverError(DriverError::UnexpectedPacket)),
        }
    }

    /// 处理MysqlNativePassword
    fn continue_mysql_native_password_auth(&mut self, auth_switched: bool) -> Result<()> {
        let payload = self.read_packet()?;

        match payload[0] {
            // auth ok
            0x00 => self.handle_ok::<CommonOkPacket>(&payload).map(drop),
            // auth switch
            0xfe if !auth_switched => {
                let auth_switch = if payload.len() > 1 {
                    ParseBuf(&payload).parse(())?
                } else {
                    let _ = ParseBuf(&payload).parse::<OldAuthSwitchRequest>(())?;
                    // we'll map OldAuthSwitchRequest to an AuthSwitchRequest with mysql_old_password plugin.
                    AuthSwitchRequest::new("mysql_old_password".as_bytes(), &*self.0.nonce)
                        .into_owned()
                };
                self.perform_auth_switch(auth_switch)
            }
            _ => Err(Error::DriverError(DriverError::UnexpectedPacket)),
        }
    }

    /// 切换认证方式
    fn perform_auth_switch(&mut self, auth_switch_request: AuthSwitchRequest<'_>) -> Result<()> {
        if matches!(
            auth_switch_request.auth_plugin(),
            AuthPlugin::MysqlOldPassword
        ) && self.0.opts.get_secure_auth()
        {
            return Err(Error::DriverError(DriverError::OldMysqlPasswordDisabled));
        }

        if matches!(
            auth_switch_request.auth_plugin(),
            AuthPlugin::Other(Cow::Borrowed(b"mysql_clear_password"))
        ) && !self.0.opts.get_enable_cleartext_plugin()
        {
            return Err(Error::DriverError(DriverError::CleartextPluginDisabled));
        }

        self.0.nonce = auth_switch_request.plugin_data().to_vec();
        self.0.auth_plugin = auth_switch_request.auth_plugin().into_owned();
        let plugin_data = match self.0.auth_plugin {
            ref x @ AuthPlugin::MysqlOldPassword => {
                if self.0.opts.get_secure_auth() {
                    return Err(Error::DriverError(DriverError::OldMysqlPasswordDisabled));
                }
                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::MysqlNativePassword => {
                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::CachingSha2Password => {
                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::MysqlClearPassword => {
                if !self.0.opts.get_enable_cleartext_plugin() {
                    return Err(Error::DriverError(DriverError::UnknownAuthPlugin(
                        "mysql_clear_password".into(),
                    )));
                }

                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            AuthPlugin::Other(_) => None,
        };

        if let Some(plugin_data) = plugin_data {
            self.write_struct(&plugin_data.into_owned())?;
        } else {
            self.write_packet(&mut &[0_u8; 0][..])?;
        }

        self.continue_auth(true)
    }

    /// 处理ok包
    fn handle_ok<'a, T: OkPacketKind>(&mut self, buffer: &'a Buffer) -> Result<OkPacket<'a>> {
        let ok = ParseBuf(buffer)
            .parse::<OkPacketDeserializer<T>>(self.0.capability_flags)?
            .into_inner();
        self.0.status_flags = ok.status_flags();
        self.0.ok_packet = Some(ok.clone().into_owned());
        Ok(ok)
    }

    /// 丢弃一个包
    fn drop_packet(&mut self) -> Result<()> {
        self.read_packet().map(drop)
    }

    /// 配置连接参数
    fn connect_attrs(&self) -> Option<HashMap<String, String>> {
        if let Some(attrs) = self.0.opts.get_connect_attrs() {
            let program_name = match attrs.get("program_name") {
                Some(program_name) => program_name.clone(),
                None => {
                    let arg0 = std::env::args_os().next();
                    let arg0 = arg0.as_ref().map(|x| x.to_string_lossy());
                    arg0.unwrap_or_else(|| "".into()).into_owned()
                }
            };

            let mut attrs_to_send = HashMap::new();

            attrs_to_send.insert("_client_name".into(), "tp-replayer".into());
            attrs_to_send.insert("_pid".into(), process::id().to_string());
            attrs_to_send.insert("program_name".into(), program_name);

            for (name, value) in attrs.clone() {
                attrs_to_send.insert(name, value);
            }

            Some(attrs_to_send)
        } else {
            None
        }
    }
}

use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;

use openssl::rsa::{Padding, Rsa};
use tracing::instrument;

use common::binlog::row::row_string::RowString;
use common::err::decode_error::ReError;
use common::err::CResult;
use common::server::Server;

use crate::bytes::xor;
use crate::commands::auth_plugin_switch_command::AuthPluginSwitchCommand;
use crate::commands::authenticate_command::AuthenticateCommand;
use crate::commands::query_command::QueryCommand;
use crate::commands::ssl_request_command::SslRequestCommand;
use crate::conn::configure::Configure;
use crate::conn::connection_options::ConnectionOptions;
use crate::conn::packet_channel::PacketChannel;
use crate::conn::query_result;
use crate::conn::query_result::StreamQueryResult;
use crate::conn::ssl_mode::SslMode;
use crate::declar::auth_plugin_names::AuthPlugin;
use crate::declar::capability_flags::CapabilityFlags;
use crate::declar::status_flags::StatusFlags;
use crate::declar::{auth_plugin_names, capability_flags, status_flags};
use crate::packet::auth_switch_packet::AuthPluginSwitchPacket;
use crate::packet::check_error_packet;
use crate::packet::handshake_packet::HandshakePacket;
use crate::packet::response_type::ResponseType;
use crate::{NULL_TERMINATOR, UTF8_MB4_GENERAL_CI};

pub trait IConnection: Server {
    fn try_connect(&mut self) -> CResult<bool>;

    fn query(&mut self, sql: String) -> CResult<Vec<RowString>>;

    /// 获得流式的查询结果
    fn query_stream(&mut self, sql: String) -> CResult<StreamQueryResult>;
}

#[derive(Debug)]
pub struct Connection {
    pub options: ConnectionOptions,

    pub configure: Configure,

    pub channel: Option<Arc<RefCell<PacketChannel>>>,

    pub transaction: bool,

    // 连接是否关闭
    is_closed: bool,

    session: Session,
}

unsafe impl Send for Connection {}

#[async_trait::async_trait]
impl Server for Connection {
    async fn start(&mut self) -> Result<(), ReError> {
        todo!()
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        todo!()
    }
}

#[derive(Debug)]
struct Session {
    // 服务能力flag
    capability_flags: CapabilityFlags,
    // 连接id
    connection_id: u32,
    // 服务器状态, 从server返回的packet中获取
    status_flags: StatusFlags,

    character_set: u8,

    server_version: String,
}

impl Session {
    pub fn default() -> Self {
        Session {
            capability_flags: CapabilityFlags::empty(),
            connection_id: 0,
            status_flags: StatusFlags::empty(),
            character_set: 0,
            server_version: String::default(),
        }
    }
}

impl IConnection for Connection {
    #[instrument]
    fn try_connect(&mut self) -> CResult<bool> {
        if self.is_closed {
            let mut channel = PacketChannel::new(&self.options)?;
            // 处理握手
            channel = Connection::do_handshake(self, channel)?;
            self.channel = Some(Arc::new(RefCell::new(channel)));

            self.is_closed = false;
        }

        Ok(true)
    }

    #[instrument]
    fn query(&mut self, sql: String) -> CResult<Vec<RowString>> {
        let command = QueryCommand::new(sql);

        let channel_rs = self.channel.as_mut();

        if channel_rs.is_none() {
            return Err(ReError::ConnectionError(String::from("channel not found")));
        }

        let channel = channel_rs.unwrap();
        channel
            .borrow_mut()
            .write_packet(&command.serialize()?, 0)?;
        let result_set = self.configure.read_result_set(channel)?;

        let mut result = Vec::<RowString>::with_capacity(result_set.len());
        for packet in result_set {
            result.push(RowString::new_row(packet.cells));
        }

        Ok(result)
    }

    fn query_stream<'a>(&'a mut self, sql: String) -> CResult<StreamQueryResult<'a>> {
        let command = QueryCommand::new(sql);
        self.write_packet(&command.serialize()?, 0)?;

        // 获取column set
        let columns = query_result::read_column_set(self)?;
        Ok(StreamQueryResult::new(self, columns.into()))
    }
}

impl Connection {
    pub fn new(options: ConnectionOptions) -> Self {
        let configure = Configure::new(options.clone());

        Self {
            options,
            configure,
            channel: None,
            transaction: false,
            is_closed: true,
            session: Session::default(),
        }
    }

    /// 进行mysql握手, ssl的情况channel会发生变更
    fn do_handshake(&mut self, mut channel: PacketChannel) -> CResult<PacketChannel> {
        // 获取server发送的第一个握手包
        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Initial handshake error.")?;
        let handshake = HandshakePacket::parse(&packet)?;

        let mut seq_num = seq_num;
        // 协议版本号0x0A/10
        if handshake.protocol_version != 10u8 {
            return Err(ReError::ConnectionError(format!(
                "Unsupported protocol version. {}",
                handshake.protocol_version
            )));
        }

        let capability_flags = CapabilityFlags::new(handshake.server_capabilities);
        // 必须是CLIENT_PROTOCOL_41
        if !capability_flags.contains(capability_flags::CLIENT_PROTOCOL_41) {
            return Err(ReError::ConnectionError(format!(
                "Protocol41 not set. {}",
                handshake.protocol_version
            )));
        }

        // 记录握手包信息
        self.handle_handshake(&handshake);

        // 如果是ssl连接，发送ssl包
        if self.options.ssl_mode != SslMode::Disabled {
            // 检查服务器是否支持ssl
            let ssl_available = capability_flags.contains(capability_flags::CLIENT_SSL);
            if !ssl_available && self.options.ssl_mode as u8 >= SslMode::Require as u8 {
                return Err(ReError::String(
                    "The server doesn't support SSL encryption".to_string(),
                ));
            }
            if ssl_available {
                let ssl_command = SslRequestCommand::new(UTF8_MB4_GENERAL_CI);
                seq_num += 1;
                channel.write_packet(&ssl_command.serialize()?, seq_num)?;
                // 切换到ssl
                channel = channel.upgrade_to_ssl(&self.options)?;
            }
        }

        // 发送握手结果并完成认证
        let auth_plugin = Connection::get_auth_plugin(&handshake.auth_plugin_name)?;
        let auth_command =
            AuthenticateCommand::new(&self.options, &handshake, auth_plugin, UTF8_MB4_GENERAL_CI);
        seq_num += 1;
        channel.write_packet(&auth_command.serialize()?, seq_num)?;

        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Authentication error.")?;
        match packet[0] {
            ResponseType::OK => return Ok(channel),
            ResponseType::AUTH_PLUGIN_SWITCH => {
                let switch_packet = AuthPluginSwitchPacket::parse(&packet[1..])?;
                Connection::handle_auth_plugin_switch(
                    &mut channel,
                    switch_packet,
                    &self.options,
                    seq_num + 1,
                )?;
                Ok(channel)
            }
            _ => {
                Connection::authenticate_sha_256(
                    &mut channel,
                    &packet,
                    &handshake.scramble,
                    &self.options.password,
                    seq_num + 1,
                )?;
                Ok(channel)
            }
        }
    }

    fn handle_auth_plugin_switch(
        channel: &mut PacketChannel,
        switch_packet: AuthPluginSwitchPacket,
        options: &ConnectionOptions,
        seq_num: u8,
    ) -> CResult<()> {
        let auth_plugin = Connection::get_auth_plugin(&switch_packet.auth_plugin_name)?;
        let auth_switch_command = AuthPluginSwitchCommand::new(
            &options.password,
            &switch_packet.auth_plugin_data,
            &switch_packet.auth_plugin_name,
            auth_plugin,
        );
        channel.write_packet(&auth_switch_command.serialize()?, seq_num)?;
        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Authentication switch error.")?;

        if switch_packet.auth_plugin_name == auth_plugin_names::CACHING_SHA2_PASSWORD {
            Connection::authenticate_sha_256(
                channel,
                &packet,
                &switch_packet.auth_plugin_data,
                &options.password,
                seq_num + 1,
            )?;
        }
        Ok(())
    }

    fn authenticate_sha_256(
        channel: &mut PacketChannel,
        packet: &[u8],
        scramble: &String,
        password: &String,
        seq_num: u8,
    ) -> CResult<()> {
        // See https://mariadb.com/kb/en/caching_sha2_password-authentication-plugin/
        // Success authentication.
        if packet[0] == 0x01 && packet[1] == 0x03 {
            return Ok(());
        }

        let mut password = password.as_bytes().to_vec();
        password.push(NULL_TERMINATOR);

        // Send clear password if ssl is used.
        if channel.is_ssl() {
            channel.write_packet(&password, seq_num)?;
            let (packet, _seq_num) = channel.read_packet()?;
            check_error_packet(&packet, "Sending clear password error.")?;
            return Ok(());
        }

        // Request public key.
        channel.write_packet(&[0x02], seq_num)?;
        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Requesting caching_sha2_password public key.")?;

        // Extract public key.
        let public_key = &packet[1..];
        let encrypted_password = xor(&password, &scramble.as_bytes());

        let rsa = Rsa::public_key_from_pem(public_key).expect("load public_key error");
        let mut encrypted_body = vec![0u8; rsa.size() as usize];
        rsa.public_encrypt(
            &encrypted_password,
            &mut encrypted_body,
            Padding::PKCS1_OAEP,
        )
        .expect("public_encrypt error");

        channel.write_packet(&encrypted_body, seq_num + 1)?;

        let (packet, _seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Authentication error.")?;
        Ok(())
    }

    fn get_auth_plugin(auth_plugin_name: &String) -> CResult<AuthPlugin> {
        if auth_plugin_name == auth_plugin_names::MY_SQL_NATIVE_PASSWORD {
            return Ok(AuthPlugin::MySqlNativePassword);
        }
        if auth_plugin_name == auth_plugin_names::CACHING_SHA2_PASSWORD {
            return Ok(AuthPlugin::CachingSha2Password);
        }

        let message = format!("{} auth plugin is not supported.", auth_plugin_name);
        Err(ReError::String(message.to_string()))
    }

    fn write_packet(&mut self, packet: &[u8], seq_num: u8) -> CResult<()> {
        let channel_rs = self.channel.as_mut();

        if channel_rs.is_none() {
            return Err(ReError::ConnectionError(String::from("channel not found")));
        }

        let channel = channel_rs.unwrap();
        channel.borrow_mut().write_packet(&packet, seq_num)
    }

    /// 读取一个mysql packet，并处理err包
    pub fn read_packet_with_check(&mut self, err_message: &str) -> CResult<(Vec<u8>, u8)> {
        let channel_rs = self.channel.as_mut();

        if channel_rs.is_none() {
            return Err(ReError::ConnectionError(String::from("channel not found")));
        }

        let channel = channel_rs.unwrap();
        let (packet, seq_num) = channel.borrow_mut().read_packet()?;
        check_error_packet(&packet, err_message)?;
        Ok((packet, seq_num))
    }

    /// 是否有更多的result, 处理MultiResultSet时使用
    pub fn more_results_exists(&self) -> bool {
        self.session
            .status_flags
            .contains(status_flags::SERVER_MORE_RESULTS_EXISTS)
    }

    /// 判定连接能力
    pub fn has_capability(&self, capability_flag: u64) -> bool {
        self.session.capability_flags.contains(capability_flag)
    }

    /// 连接发生异常，清空部分连接数据
    fn handle_err(&mut self) {
        self.session.status_flags = StatusFlags::empty();
    }

    /// 记录握手包中获得的server信息
    fn handle_handshake(&mut self, hp: &HandshakePacket) {
        self.session.capability_flags =
            CapabilityFlags::new(hp.server_capabilities & self.get_client_flags());
        self.session.status_flags = StatusFlags::new(hp.status_flags);
        self.session.connection_id = hp.connection_id;
        self.session.character_set = hp.server_collation;
        self.session.server_version = hp.server_version.clone();
    }

    /// 获得client能力flag
    fn get_client_flags(&self) -> u64 {
        let mut client_flags = capability_flags::CLIENT_PROTOCOL_41
            | capability_flags::CLIENT_SECURE_CONNECTION
            | capability_flags::CLIENT_LONG_PASSWORD
            | capability_flags::CLIENT_TRANSACTIONS
            | capability_flags::CLIENT_LOCAL_FILES
            | capability_flags::CLIENT_MULTI_STATEMENTS
            | capability_flags::CLIENT_MULTI_RESULTS
            | capability_flags::CLIENT_PS_MULTI_RESULTS
            | capability_flags::CLIENT_PLUGIN_AUTH
            | capability_flags::CLIENT_LONG_FLAG;
        return client_flags;
    }
}

#[cfg(test)]
mod test {
    use crate::conn::connection::{Connection, IConnection};
    use crate::conn::connection_options::ConnectionOptions;

    #[test]
    fn test_conn() {
        let mut opts = ConnectionOptions::default();
        opts.update_auth(String::from("root"), String::from("123456"));

        let mut conn = Connection::new(opts);
        let channel_rs = conn.try_connect();
        assert!(channel_rs.is_ok());

        let query = conn
            .query(String::from("select 1+ 1"))
            .expect("test_conn error");
        let values = &query[0].as_slice();
        assert_eq!(values[0].clone().unwrap(), "2")
    }
}

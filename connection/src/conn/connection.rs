use std::cell::RefCell;
use std::sync::Arc;
use openssl::rsa::{Padding, Rsa};
use tracing::instrument;
use common::err::CResult;
use common::err::decode_error::ReError;
use common::row::row_string::RowString;
use crate::commands::auth_plugin_switch_command::AuthPluginSwitchCommand;
use crate::commands::authenticate_command::AuthenticateCommand;
use crate::commands::ssl_request_command::SslRequestCommand;
use crate::declar::{auth_plugin_names, capability_flags};
use crate::declar::auth_plugin_names::AuthPlugin;
use crate::packet::auth_switch_packet::AuthPluginSwitchPacket;
use crate::packet::check_error_packet;
use crate::packet::handshake_packet::HandshakePacket;
use crate::packet::response_type::ResponseType;
use crate::conn::ssl_mode::SslMode;
use crate::{NULL_TERMINATOR, UTF8_MB4_GENERAL_CI};
use crate::bytes::xor;
use crate::commands::query_command::QueryCommand;
use crate::conn::configure::Configure;
use crate::conn::connection_options::ConnectionOptions;
use crate::conn::packet_channel::PacketChannel;

pub trait IConnection {

    fn try_connect(&mut self) -> CResult<bool>;

    fn query(&mut self, sql: String) -> CResult<Vec<RowString>>;

}

#[derive(Debug)]
pub struct Connection {
    pub options: ConnectionOptions,

    pub configure: Configure,

    pub channel: Option<Arc<RefCell<PacketChannel>>>,

    pub transaction: bool,

}

impl IConnection for Connection {

    #[instrument]
    fn try_connect(&mut self) -> CResult<bool> {
        let mut channel = PacketChannel::new(&self.options)?;
        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Initial handshake error.")?;
        let handshake = HandshakePacket::parse(&packet)?;

        let auth_plugin = self.get_auth_plugin(&handshake.auth_plugin_name)?;
        self.authenticate(&mut channel, &handshake, auth_plugin, seq_num + 1)?;

        self.channel = Some(Arc::new(RefCell::new(channel)));

        Ok(true)
    }

    #[instrument]
    fn query(&mut self, sql: String) -> CResult<Vec<RowString>> {
        let command = QueryCommand::new(sql);

        let channel_rs = self.channel.as_mut();

        if channel_rs.is_none() {
            return Err(ReError::ConnectionError(String::from("channel not found")))
        }

        let channel = channel_rs.unwrap();
        channel.borrow_mut().write_packet(&command.serialize()?, 0)?;
        let result_set = self.configure.read_result_set(channel)?;

        let mut result = Vec::<RowString>::with_capacity(result_set.len());
        for packet in result_set {
            result.push(RowString::new_row(packet.cells));
        }

        Ok(result)
    }
}

impl Connection {
    pub fn new(options: ConnectionOptions) -> Self {
        if options.ssl_mode != SslMode::Disabled {
            unimplemented!("Ssl encryption is not supported in this version");
        }

        let configure = Configure::new(options.clone());

        Self {
            options,
            configure,
            channel: None,
            transaction: false,
        }
    }

    fn authenticate(
        &self,
        channel: &mut PacketChannel,
        handshake: &HandshakePacket,
        auth_plugin: AuthPlugin,
        mut seq_num: u8,
    ) -> CResult<()> {
        let mut use_ssl = false;
        if self.options.ssl_mode != SslMode::Disabled {
            let ssl_available = (handshake.server_capabilities & capability_flags::CLIENT_SSL) != 0;
            if !ssl_available && self.options.ssl_mode as u8 >= SslMode::Require as u8 {
                return Err(ReError::String(
                    "The server doesn't support SSL encryption".to_string(),
                ));
            }
            if ssl_available {
                let ssl_command = SslRequestCommand::new(UTF8_MB4_GENERAL_CI);
                channel.write_packet(&ssl_command.serialize()?, seq_num)?;
                seq_num += 1;
                channel.upgrade_to_ssl();
                use_ssl = true;
            }
        }

        let auth_command =
            AuthenticateCommand::new(&self.options, handshake, auth_plugin, UTF8_MB4_GENERAL_CI);
        channel.write_packet(&auth_command.serialize()?, seq_num)?;
        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Authentication error.")?;

        match packet[0] {
            ResponseType::OK => return Ok(()),
            ResponseType::AUTH_PLUGIN_SWITCH => {
                let switch_packet = AuthPluginSwitchPacket::parse(&packet[1..])?;
                self.handle_auth_plugin_switch(channel, switch_packet, seq_num + 1, use_ssl)?;
                Ok(())
            }
            _ => {
                self.authenticate_sha_256(
                    channel,
                    &packet,
                    &handshake.scramble,
                    seq_num + 1,
                    use_ssl,
                )?;
                Ok(())
            }
        }
    }

    fn handle_auth_plugin_switch(
        &self,
        channel: &mut PacketChannel,
        switch_packet: AuthPluginSwitchPacket,
        seq_num: u8,
        use_ssl: bool,
    ) -> CResult<()> {
        let auth_plugin = self.get_auth_plugin(&switch_packet.auth_plugin_name)?;
        let auth_switch_command = AuthPluginSwitchCommand::new(
            &self.options.password,
            &switch_packet.auth_plugin_data,
            &switch_packet.auth_plugin_name,
            auth_plugin,
        );
        channel.write_packet(&auth_switch_command.serialize()?, seq_num)?;
        let (packet, seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Authentication switch error.")?;

        if switch_packet.auth_plugin_name == auth_plugin_names::CACHING_SHA2_PASSWORD {
            self.authenticate_sha_256(
                channel,
                &packet,
                &switch_packet.auth_plugin_data,
                seq_num + 1,
                use_ssl,
            )?;
        }
        Ok(())
    }

    fn authenticate_sha_256(
        &self,
        channel: &mut PacketChannel,
        packet: &[u8],
        scramble: &String,
        seq_num: u8,
        use_ssl: bool,
    ) -> CResult<()> {
        // See https://mariadb.com/kb/en/caching_sha2_password-authentication-plugin/
        // Success authentication.
        if packet[0] == 0x01 && packet[1] == 0x03 {
            return Ok(());
        }

        let mut password = self.options.password.as_bytes().to_vec();
        password.push(NULL_TERMINATOR);

        // Send clear password if ssl is used.
        if use_ssl {
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
        ).expect("public_encrypt error");

        channel.write_packet(&encrypted_body, seq_num + 1)?;

        let (packet, _seq_num) = channel.read_packet()?;
        check_error_packet(&packet, "Authentication error.")?;
        Ok(())
    }

    fn get_auth_plugin(&self, auth_plugin_name: &String) -> CResult<AuthPlugin> {
        if auth_plugin_name == auth_plugin_names::MY_SQL_NATIVE_PASSWORD {
            return Ok(AuthPlugin::MySqlNativePassword);
        }
        if auth_plugin_name == auth_plugin_names::CACHING_SHA2_PASSWORD {
            return Ok(AuthPlugin::CachingSha2Password);
        }

        let message = format!("{} auth plugin is not supported.", auth_plugin_name);
        Err(ReError::String(message.to_string()))
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

        let query = conn.query(String::from("select 1+ 1")).expect("test_conn error");
        let values = &query[0].as_slice();
        assert_eq!(values[0], "2")
    }
}

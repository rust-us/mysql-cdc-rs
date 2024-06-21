use std::io::Cursor;
use binlog::utils::{read_null_term_string_with_cursor};
use common::err::CResult;
use common::err::decode_error::ReError;

#[derive(Debug)]
pub struct AuthPluginSwitchPacket {
    pub auth_plugin_name: String,
    pub auth_plugin_data: String,
}

impl AuthPluginSwitchPacket {
    pub fn parse(packet: &[u8]) -> CResult<Self> {
        let mut cursor = Cursor::new(packet);

        let auth_plugin_name = read_null_term_string_with_cursor(&mut cursor)?;
        let auth_plugin_data = read_null_term_string_with_cursor(&mut cursor)?;

        Ok(Self {
            auth_plugin_name,
            auth_plugin_data,
        })
    }
}

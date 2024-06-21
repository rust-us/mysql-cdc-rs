use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};

use common::err::CResult;

use crate::packet::response_type::ResponseType;

#[derive(Debug)]
pub struct EndOfFilePacket {
    pub warning_count: u16,
    pub server_status: u16,
}

impl EndOfFilePacket {
    pub fn parse(packet: &[u8]) -> CResult<Self> {
        let mut cursor = Cursor::new(packet);

        let warning_count = cursor.read_u16::<LittleEndian>()?;
        let server_status = cursor.read_u16::<LittleEndian>()?;

        Ok(Self {
            warning_count,
            server_status,
        })
    }

    pub fn is_eof(packet: &[u8]) -> bool {
        // [fe]也可能出现在LengthEncodedInteger，必须检查长度<9确保是EOF
        if packet.len() <= 0 || packet.len() >= 9 {
            return false;
        }
        packet[0] == ResponseType::END_OF_FILE
    }
}

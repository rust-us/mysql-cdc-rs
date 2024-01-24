use std::io;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use common::err::CResult;

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
}

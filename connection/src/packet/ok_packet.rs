use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;

use binlog::utils::read_len_enc_num;
use common::err::CResult;

use crate::declar::status_flags::StatusFlags;

#[derive(Debug)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: StatusFlags,
    pub warnings: u16,
    pub info: String,
}

impl OkPacket {
    pub fn parse(packet: &[u8]) -> CResult<Self> {
        let mut cursor = Cursor::new(packet);

        let header = cursor.read_u8()?;
        let mut affected_rows = 0;
        if cursor.has_remaining() {
            affected_rows = read_len_enc_num(&mut cursor)?.1;
        }
        let mut last_insert_id = 0;
        if cursor.has_remaining() {
            last_insert_id = read_len_enc_num(&mut cursor)?.1;
        }
        let mut status = 0u16;
        if cursor.remaining() >= 2 {
            status = cursor.read_u16::<LittleEndian>()?;
        }
        let mut warnings = 0u16;
        if cursor.remaining() >= 2 {
            warnings = cursor.read_u16::<LittleEndian>()?;
        }
        let mut info = String::new();
        cursor.read_to_string(&mut info)?;

        Ok(Self {
            affected_rows,
            last_insert_id,
            status_flags: StatusFlags::new(status),
            warnings,
            info,
        })
    }
}

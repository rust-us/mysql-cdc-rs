use std::io::Cursor;

use binlog::utils::read_len_enc_str_with_cursor_allow_null;
use common::err::CResult;

#[derive(Debug)]
pub struct ResultSetRowPacket {
    pub cells: Vec<Option<String>>,
}

impl ResultSetRowPacket {
    pub fn parse(packet: &[u8]) -> CResult<Self> {
        let mut cursor = Cursor::new(packet);

        let len = cursor.get_ref().len() as u64;
        let mut cells = Vec::new();

        while cursor.position() < len {
            cells.push(read_len_enc_str_with_cursor_allow_null(&mut cursor)?);
        }

        Ok(Self { cells })
    }
}

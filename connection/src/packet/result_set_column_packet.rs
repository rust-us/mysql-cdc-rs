use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};

use binlog::utils::{read_len_enc_num, read_len_enc_str_with_cursor};
use common::err::CResult;

#[derive(Debug, Clone)]
pub struct ResultSetColumnPacket {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    pub next_length: u64,
    pub character_set: u16,
    pub column_length: u32,
    pub column_type: u8,
    pub flags: u16,
    pub decimals: u8,
    pub __filler: u16,
    // COM_FIELD_LIST is deprecated, so we won't support it
}

impl ResultSetColumnPacket {
    pub fn parse(packet: &[u8]) -> CResult<Self> {
        let mut cursor = Cursor::new(packet);

        let catalog = read_len_enc_str_with_cursor(&mut cursor)?;
        let schema = read_len_enc_str_with_cursor(&mut cursor)?;
        let table = read_len_enc_str_with_cursor(&mut cursor)?;
        let org_table = read_len_enc_str_with_cursor(&mut cursor)?;
        let name = read_len_enc_str_with_cursor(&mut cursor)?;
        let org_name = read_len_enc_str_with_cursor(&mut cursor)?;
        let next_length = read_len_enc_num(&mut cursor)?.1;
        let character_set = cursor.read_u16::<LittleEndian>()?;
        let column_length = cursor.read_u32::<LittleEndian>()?;
        let column_type = cursor.read_u8()?;
        let flags = cursor.read_u16::<LittleEndian>()?;
        let decimals = cursor.read_u8()?;

        Ok(Self {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            next_length,
            character_set,
            column_length,
            column_type,
            flags,
            decimals,
            __filler: 0u16,
        })
    }
}

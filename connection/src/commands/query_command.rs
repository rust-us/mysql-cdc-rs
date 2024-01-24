use std::io;
use std::io::{Cursor, Write};
use byteorder::WriteBytesExt;
use crate::commands::command::CommandType;

pub struct QueryCommand {
    pub sql: String,
}

impl QueryCommand {
    pub fn new(sql: String) -> Self {
        Self { sql }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, io::Error> {
        let mut vec = Vec::new();
        let mut cursor = Cursor::new(&mut vec);

        cursor.write_u8(CommandType::Query as u8)?;
        cursor.write(self.sql.as_bytes())?;

        Ok(vec)
    }
}

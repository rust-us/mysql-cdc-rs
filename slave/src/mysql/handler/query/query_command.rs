use bytes::BufMut;
use mysql_common::constants::Command;
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::bytes::EofBytes;
use mysql_common::misc::raw::{Const, RawBytes};
use mysql_common::proto::{MyDeserialize, MySerialize};
use std::io;

/// query command
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCommand<'a> {
    cmd: Const<Command, u8>,
    query: RawBytes<'a, EofBytes>,
}

impl<'a> QueryCommand<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            cmd: Const::new(Command::COM_QUERY),
            query: RawBytes::new(sql.as_bytes()),
        }
    }
}

impl MySerialize for QueryCommand<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(*self.cmd as u8);
        self.query.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for QueryCommand<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        // 处理cmd byte
        let _ = buf.parse_unchecked::<[u8; 1]>(())?;
        Ok(Self {
            cmd: Const::new(Command::COM_QUERY),
            query: buf.parse(())?,
        })
    }
}

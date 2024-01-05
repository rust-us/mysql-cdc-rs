use std::io::Cursor;
use std::mem::size_of;

use byteorder::{BigEndian, ByteOrder};

use memory::Buffer;

use crate::cmd::{Decoder, Encoder};
use crate::error::{err_if, HResult};
use crate::error::Error::RaftCommandParseRemotingHeaderErr;
use crate::message_type::MessageType;

pub struct RemotingHeader {
    version: i32,
    code: i8,
    msg_type: MessageType,
    opaque: i32,
    flag: i8,
    remark_nullable: bool,
    remark: Option<String>,
    seq_no: i32,
    last: bool,
    // custom_header
}

impl Decoder for RemotingHeader {
    fn decode(buf: &mut [u8]) -> HResult<(Self, usize)> {
        let mut pos = 0;
        let version = BigEndian::read_i32(buf);
        pos += size_of::<i32>();
        let code = buf[pos] as i8;
        pos += 1;
        let msg_type_value = BigEndian::read_i32(&buf[pos..]);
        let msg_type = MessageType::try_from(msg_type_value).map_err(|_|RaftCommandParseRemotingHeaderErr)?;
        pos += size_of::<i32>();
        let opaque = BigEndian::read_i32(&buf[pos..]);
        pos += size_of::<i32>();
        let flag = buf[pos] as i8;
        pos += 1;
        let remark_nullable = buf[pos] != 0;
        err_if!(RaftCommandParseRemotingHeaderErr, remark_nullable);
        pos += 1;
        let seq_no = BigEndian::read_i32(&buf[pos..]);
        pos += size_of::<i32>();
        let last = buf[pos] != 0;
        pos += 1;
        let custom_header = buf[pos] != 0;
        err_if!(RaftCommandParseRemotingHeaderErr, custom_header);
        let h = Self {
            version,
            code,
            msg_type,
            opaque,
            flag,
            remark_nullable,
            remark: None,
            seq_no,
            last
        };
        Ok((h, pos))
    }
}

impl Encoder for RemotingHeader {
    fn encode(&self, data: &mut Buffer) -> HResult<usize> {
        let mut pos = 0;
        pos += data.write_int(self.version)?;
        pos += data.write_byte(self.code)?;
        pos += data.write_int(self.msg_type.into())?;
        pos += data.write_int(self.opaque)?;
        pos += data.write_byte(self.flag)?;
        if let Some(remark) = &self.remark {
            pos += data.write_bool(false)?;
            pos += data.write_utf8(remark.as_str())?;
        } else {
            pos += data.write_bool(true)?;
        }
        pos += data.write_int(self.seq_no)?;
        pos += data.write_bool(self.last)?;
        // no customHeader
        pos += data.write_bool(true)?;
        Ok(pos)
    }
}
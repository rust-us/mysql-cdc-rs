use std::mem::size_of;

use byteorder::{BE, ByteOrder};

use crate::cmd::Decoder;
use crate::error::Error::RaftParseAddrErr;
use crate::error::HResult;
use crate::serde::raft_read_utf8;

pub struct Address {
    host: String,
    port: i32,
}

impl Decoder for Address {
    fn decode(buf: &[u8]) -> HResult<(Self, usize)> {
        let (host, mut pos) = match raft_read_utf8(buf)? {
            ((Some(host), pos)) => {
                (host, pos)
            }
            ((None, _)) => {
                return Err(RaftParseAddrErr);
            }
        };
        let port = BE::read_i32(&buf[pos..]);
        pos += size_of::<i32>();
        Ok((Self { host, port }, pos))
    }
}

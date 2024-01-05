use memory::Buffer;

use crate::cmd::{Decoder, Encoder};
use crate::error::HResult;
use crate::serde::{Address, BaseResponse, raft_write_utf8, read_address, read_address_list};

pub struct ConnectRequest {
    pub client: String,
}

pub struct ConnectResponse {
    pub base: BaseResponse,
    pub leader: Address,
    pub members: Vec<Address>,
}

impl Encoder for ConnectRequest {
    #[inline]
    fn encode(&self, buf: &mut Buffer) -> HResult<usize> {
        raft_write_utf8(Some(&self.client), buf)
    }
}

impl Decoder for ConnectResponse {
    fn decode(buf: &[u8]) -> HResult<(Self, usize)> {
        let (base, p0) = BaseResponse::decode(buf)?;
        if base.has_err() {  }
        let (leader, p1) = read_address(&buf[p0..])?;
        let (members, p2) = read_address_list(&buf[p0+p1..])?;
        Ok((Self { base, leader, members}, p0+p1+p2))
    }
}
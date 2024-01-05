use memory::Buffer;

use crate::cmd::{Command, Decoder, Encoder, RequestType};
use crate::cmd::remoting_header::RemotingHeader;
use crate::error::HResult;

pub struct RemotingCommand {
    header: RemotingHeader,
    req_type: RequestType,
    cmd: Command,
}

impl Encoder for RemotingCommand {
    fn encode(&self, data: &mut Buffer) -> HResult<usize> {
        data.skip_bytes(8)?;
        let header_len = self.header.encode(data)?;
        let body_len = self.cmd.encode(data)?;
        let (seg_idx, seg_offset) = data.position();
        data.reset();
        data.write_int((8 + header_len + body_len) as i32)?;
        data.write_int(header_len as i32)?;
        unsafe {
            data.set_position(seg_idx, seg_offset);
        }
        Ok(data.length())
    }
}

impl Decoder for RemotingCommand {
    fn decode(buf: &mut [u8]) -> HResult<(Self, usize)> {
        todo!()
    }
}
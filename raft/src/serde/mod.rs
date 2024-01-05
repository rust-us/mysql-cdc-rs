use std::mem::size_of;

use byteorder::{BE, ByteOrder};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use paste::paste;

use memory::Buffer;

use crate::cmd::{CopyCatError, Decoder, Identifier, identifier_code, ResponseStatus};
use crate::error::{err_if, Error, HResult};
use crate::error::Error::{RaftCommandParseUtf8Err, RaftParseCopyCatErrorErr, RaftParseStatusErr};
pub use crate::serde::address::Address;
use crate::serde::connect::ConnectRequest;

pub mod address;
pub mod connect;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Copy, Clone)]
#[repr(i32)]
pub enum TypeSerializer {
    Address = -1,

    CommandRequest = -3,
    CommandResponse = -10,

    ConnectRequest = -4,
    ConnectResponse = -11,

    KeepAliveRequest = -5,
    KeepAliveResponse = -12,

    RegisterRequest = -8,
    RegisterResponse = -15,

    UnRegisterRequest = -9,
    UnRegisterResponse = -16,

    NoOpCommand = -45,
}

pub struct BaseResponse {
    status: ResponseStatus,
    error: Option<CopyCatError>,
}

macro_rules! define_read {
    ($name: ident, $st: ty, $ty_ser: pat, $err: expr) => {
        paste! {
            pub fn [<read_ $name>](buf: &[u8]) -> HResult<($st,usize)> {
                let code = Identifier::try_from(buf[0] as i8)
                    .map_err(|_| Error::RaftIdentifierUnknown)?;
                let (serializer_code, p0) = code.read(buf)?;
                let serializer = TypeSerializer::try_from(serializer_code)
                .map_err(|_| Error::RaftTypeSerializerErr)?;
                if !matches!(serializer, $ty_ser) {
                    return Err($err);
                }
                let(addr, p1) = <$st>::decode(&buf[p0..])?;
                Ok((addr, p0+p1))
            }
        }
    };
}

macro_rules! define_read_list {
    ($name: ident, $st: ty, $ty_ser: pat, $err: expr) => {
        paste! {
            pub fn [<read_ $name _list>](buf: &[u8]) -> HResult<(Vec<$st>,usize)> {
                let len = BE::read_u16(buf) as usize;
                let mut v = Vec::with_capacity(len);
                let mut pos = size_of::<u16>();
                for i in 0..len {
                    let (t, p0) = [<read_ $name>](buf)?;
                    v.push(t);
                    pos += p0;
                }
                Ok((v, pos))
            }
        }
    };
}

macro_rules! define_read_all {
    ($name: ident, $st: ty, $ty_ser: pat, $err: expr) => {
        define_read!($name, $st, $ty_ser, $err);
        define_read_list!($name, $st, $ty_ser, $err);
    };
}


use crate::cmd::Encoder;
macro_rules! define_write {
    ($name: ident, $st: ty, $ty_ser: pat, $err: expr) => {
        paste! {
            pub fn [<write_ $name>](this: &$st, buf: &mut Buffer) -> HResult<usize> {
                let serializer_id: i32 = $ty_ser.into();
                let identifier_id: i8 = identifier_code(serializer_id);
                let identifier = Identifier::try_from(identifier_id).map_err(|_| Error::RaftIdentifierUnknown)?;
                let mut pos = 0;
                pos += buf.write_byte(identifier_id)?;
                pos += identifier.write(serializer_id, buf)?;
                pos += this.encode(buf)?;
                Ok(pos)
            }
        }
    };
}

define_read_all!(address, Address, TypeSerializer::Address, Error::RaftParseAddrErr);

define_write!(connect_request, ConnectRequest, TypeSerializer::ConnectRequest, Error::RaftEncodeOutOfMemory);

impl BaseResponse {
    #[inline]
    pub fn has_err(&self) -> bool {
        matches!(self.status, ResponseStatus::Ok)
    }
}

impl Decoder for BaseResponse {
    fn decode(buf: &[u8]) -> HResult<(Self, usize)> {
        let status_code = buf[0] as i8;
        let status = ResponseStatus::try_from(status_code).map_err(|_| RaftParseStatusErr)?;
        if matches!(status, ResponseStatus::Ok) {
            Ok((Self {
                status,
                error: None,
            }, 1))
        } else {
            let error_code = buf[1] as i8;
            let error = CopyCatError::try_from(error_code)
                .map_err(|_| RaftParseCopyCatErrorErr)?;
            Ok((Self {
                status,
                error: Some(error)
            }, 2))
        }
    }
}

pub fn raft_read_utf8(buf: &[u8]) -> HResult<(Option<String>, usize)> {
    err_if!(RaftParseCopyCatErrorErr, buf.len() < 1);
    let null = buf[0] as i8;
    if null == 0 {
        return Ok((None, 1));
    }
    let len = BE::read_u16(&buf[1..]) as usize;
    err_if!(RaftCommandParseUtf8Err, buf.len() < 3+len);
    // len must be odd
    let utf8 = Vec::from(&buf[3..3 + len]);
    let s = String::from_utf8(utf8).map_err(|_| RaftCommandParseUtf8Err)?;
    Ok((Some(s), 3 + len))
}

pub fn raft_write_utf8(str: Option<&String>, buf: &mut Buffer) -> HResult<usize> {
    if let Some(s) = str {
        buf.write_byte(1)?;
        buf.write_short(s.len() as i16)?;
        buf.write_bytes(s.as_bytes())?;
        Ok(3 + buf.length())
    } else {
        buf.write_byte(0)?;
        Ok(1)
    }
}

#[cfg(test)]
mod test {
    use std::mem::transmute;

    use crate::serde::raft_read_utf8;

    #[test]
    fn test_str0() {
        let bin = [1, 0, 6, 49, 50, 51, 97, 98, 99];
        let (s, len) = raft_read_utf8(bin.as_slice()).unwrap();
        assert_eq!(bin.len(), len);
        assert_eq!("123abc", s.unwrap());
    }

    #[test]
    fn test_str1() {
        let bin = [1, 0, 33, 49, 57, 42, 33, 38, 64, 35, 42, 35, 40, 41, 36, 65, 75, 83, 74, 68, 122, 117, 104, 97, 115, 100, 110, 106, 58, 60, 62, 63, 34, 97, 115, 100];
        let (s, len) = raft_read_utf8(bin.as_slice()).unwrap();
        assert_eq!(bin.len(), len);
        assert_eq!("19*!&@#*#()$AKSJDzuhasdnj:<>?\"asd", s.unwrap());
    }

    #[test]
    fn test_str2() {
        let bin: [u8; 9] = unsafe {
            transmute([1_i8, 0, 6, -26, -79, -119, -27, -83, -105])
        };
        let (s, len) = raft_read_utf8(bin.as_slice()).unwrap();
        assert_eq!(bin.len(), len);
        assert_eq!("汉字", s.unwrap());
    }

    #[test]
    fn test_number() {
        let b = [255, 255, 255, 252];
        let v: i32 = i32::from_be_bytes(b);
        println!("v: {}", v);
    }
}
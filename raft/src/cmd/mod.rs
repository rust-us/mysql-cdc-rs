use std::mem::size_of;

use byteorder::{BE, ByteOrder};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use memory::Buffer;

use crate::error::{Error, HResult};
use crate::error::Error::RaftIdentifierClassNotSupport;
use crate::serde::{TypeSerializer, write_connect_request};
use crate::serde::connect::{ConnectRequest, ConnectResponse};

pub const REQUEST_MAGIC: i64 = -99999999;
pub const VERSION: i32 = 4;

pub trait Encoder {
    fn encode(&self, buf: &mut Buffer) -> HResult<usize>;
}

pub trait Decoder: Sized {
    fn decode(buf: &[u8]) -> HResult<(Self, usize)>;
}

pub enum Command {
    None,
    ConnectRequest(ConnectRequest),
    ConnectResponse(ConnectResponse),
}

#[derive(TryFromPrimitive, Debug, Copy, Clone)]
#[repr(i8)]
pub enum ResponseStatus {
    Error = 0,
    Ok = 1,
}

#[derive(TryFromPrimitive, Debug, Copy, Clone)]
#[repr(i8)]
pub enum CopyCatError {
    NoLeaderError = 1,
    QueryError = 2,
    CommandError = 3,
    ApplicationError = 4,
    IllegalMemberStateError = 5,
    UnknownSessionError = 6,
    InternalError = 7,
    ConfigurationError = 8,
    LeaderTransfer = 9,
}

#[derive(IntoPrimitive, Debug, Copy, Clone)]
#[repr(i8)]
pub enum PackageType {
    REQUEST = 1,
    RESPONSE = 2,
    SUCCESS = 3,
    FAILURE = 4,
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Copy, Clone)]
#[repr(i8)]
pub enum Identifier {
    Null = 0,
    Int8 = 1,
    UINT8 = 2,
    INT16 = 3,
    UINT16 = 4,
    INT24 = 5,
    UINT24 = 6,
    INT32 = 7,
    Class = 8,
}

impl Identifier {
    pub fn read(&self, buf: &[u8]) -> HResult<(i32, usize)> {
        match self {
            Identifier::Null => Ok((0, 0)),
            Identifier::Int8 => {
                Ok((buf[0] as i8 as i32, 1))
            },
            Identifier::UINT8 => {
                Ok((buf[0] as i32, 1))
            },
            Identifier::INT16 => {
                Ok((BE::read_i16(buf) as i32, 2))
            }
            Identifier::UINT16 => {
                Ok((BE::read_i16(buf) as i32, 2))
            }
            Identifier::INT24 => {
                Ok((BE::read_i24(buf), 3))
            }
            Identifier::UINT24 => {
                Ok((BE::read_u24(buf) as i32, 3))
            }
            Identifier::INT32 => {
                Ok((BE::read_i32(buf), size_of::<i32>()))
            }
            Identifier::Class => {
                Err(RaftIdentifierClassNotSupport)
            }
        }
    }
    pub fn write(&self, value: i32, buf: &mut Buffer) -> HResult<usize> {
        match self {
            Identifier::Null => {
                Ok(0)
            }
            Identifier::Int8 => {
                buf.write_byte(value as i8).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::UINT8 => {
                buf.write_byte(value as u8 as i8).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::INT16 => {
                buf.write_short(value as i16).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::UINT16 => {
                buf.write_short(value as u16 as i16).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::INT24 => {
                buf.write_i24(value).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::UINT24 => {
                buf.write_u24(value).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::INT32 => {
                buf.write_int(value).map_err(|_| Error::RaftEncodeOutOfMemory)
            }
            Identifier::Class => {
                todo!()
            }
        }
    }
}

pub fn identifier_code(value: i32) -> i8 {
    if value >= i8::MIN as _ && value <= i8::MAX as _ {
        Identifier::Int8.into()
    } else if value >= 0 && value <= 255 {
        Identifier::UINT8.into()
    } else if value >= i16::MIN as _ && value <= i16::MAX as _ {
        Identifier::INT16.into()
    } else if value >= u16::MIN as _ && value <= u16::MAX as _ {
        Identifier::UINT16.into()
    } else if value >= -8388608 && value <= 8388607 {
        Identifier::INT24.into()
    } else if value >= 0 && value <= 16777215 {
        Identifier::UINT24.into()
    } else {
        Identifier::INT32.into()
    }
}

pub struct Insert {}

pub struct Update {}

pub struct Delete {}

impl Encoder for Command {
    fn encode(&self, buf: &mut Buffer) -> HResult<usize> {
        match self {
            Command::ConnectRequest(req) => {
                write_connect_request(req, buf)
            }
            _ => {
                todo!()
            }
        }
    }
}

impl Decoder for Command {
    fn decode(buf: &[u8]) -> HResult<(Self, usize)> {
        let identifier_code = buf[0] as i8;
        let identifier = Identifier::try_from(identifier_code).map_err(|_| Error::RaftIdentifierUnknown)?;
        let mut pos = 1;
        let (serializer_id, p0) = identifier.read(&buf[pos..])?;
        pos += p0;
        let serializer = TypeSerializer::try_from(serializer_id).map_err(|_| Error::RaftTypeSerializerErr)?;
        match serializer {
            TypeSerializer::ConnectResponse => {
                let (r, p1) = ConnectResponse::decode(&buf[pos..])?;
                pos += p1;
                Ok((Command::ConnectResponse(r), pos))
            },
            _other => {
                Err(Error::KnownCommand)
            }
        }
    }
}
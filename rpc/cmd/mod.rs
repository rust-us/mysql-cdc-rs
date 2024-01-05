pub mod remoting_command;
pub mod remoting_header;

use num_enum::IntoPrimitive;
use memory::Buffer;
use crate::error::HResult;

pub const REQUEST_MAGIC: i64 = -99999999;
pub const VERSION: i32 = 4;

pub trait Encoder {
    fn encode(&self, data: &mut Buffer) -> HResult<usize>;
}

pub trait Decoder {
    fn decode(buf: &mut [u8]) -> HResult<(Self, usize)>;
}

#[derive(IntoPrimitive, Debug, Copy, Clone)]
#[repr(i32)]
pub enum RequestType {
    Connect = -4,
    Control = 6,
    CreateTable = 2,
    KeepAlive = -5,
    Command = -3,
    Register = -8,
}

pub enum Command {
    None,
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

#[derive(IntoPrimitive, Debug, Copy, Clone)]
#[repr(i8)]
pub enum PackageType {
    REQUEST = 1,
    RESPONSE = 2,
    SUCCESS = 3,
    FAILURE = 4,
}

#[derive(IntoPrimitive, Debug, Copy, Clone)]
#[repr(i32)]
pub enum TypeSerializer {
    KeepAliveRequest = -5,
}

#[derive(IntoPrimitive, Debug, Copy, Clone)]
#[repr(i8)]
pub enum Identifier {
    Int8 = 1,
    UINT8 = 2,
    INT16 = 3,
    UINT16 = 4,
    INT24 = 5,
    UINT24 = 6,
    INT32 = 7,
    NULL = 8,

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
    fn encode(&self, data: &mut Buffer) -> HResult<usize> {
        todo!()
    }
}
use std::alloc::AllocError;
use num_enum::IntoPrimitive;
use crate::error::Error::{IoErr, OutOfMemory};

pub type HResult<T> = Result<T, Error>;

#[derive(IntoPrimitive, Copy, Clone, Debug)]
#[repr(i32)]
pub enum Error {
    OutOfMemory = 10000,
    IoErr = 10001,

    RaftCommandParseFrameLenErr = 11000,
    RaftCommandParseFrameErr = 11001,
    RaftCommandParseRemotingHeaderErr = 11002,
    RaftCommandParseUtf8Err = 11003,
    RaftParseStatusErr = 11004,
    RaftParseCopyCatErrorErr = 11005,
    RaftParseAddrErr = 11006,
    RaftIdentifierClassNotSupport = 11007,
    RaftIdentifierUnknown = 11008,
    RaftTypeSerializerErr = 11009,

    RaftParseBodyErr = 11010,

    KnownCommand = 13000,
    RaftEncodeOutOfMemory = 13001,

}

impl From<AllocError> for Error {
    fn from(_value: AllocError) -> Self {
        OutOfMemory
    }
}

impl From<std::io::Error> for Error {
    fn from(_value: std::io::Error) -> Self {
        IoErr
    }
}

macro_rules! err_if {
    ($code: ident, $if_t: expr) => {
        if $if_t {
            return Err(crate::error::Error::$code);
        }
    };
}

pub(crate) use err_if;

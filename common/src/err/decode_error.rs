use std::fmt::Display;
use std::{fmt, io};
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use hex::FromHexError;

#[derive(Debug)]
pub enum ReError {
    //////////////////////
    // Common
    //////////////////////
    /// 一定不会出现的异常。如果出现，一定是BUG
    BUG(String),
    /// The parser had an error (recoverable)
    Error(String),

    //////////////////////
    // SQL Parser
    //////////////////////
    ASTParserError(String),

    //////////////////////
    // Binlog
    //////////////////////
    /// Byte code is incomplete
    /// 此错误用于binlog编解码过程中的异常处理，包含：
    ///     编解码进行中、已完成、格式错误等， 由 Needed 产生为具体的错误信息描述
    Incomplete(Needed),

    //////////////////////
    // IO
    //////////////////////
    IoError(io::Error),
    Utf8Error(Utf8Error),
    FromUtf8Error(FromUtf8Error),
    FromHexError(FromHexError),
    ParseIntError(ParseIntError),
    ConnectionError(String),
    String(String),

    /// The parser had an unrecoverable error: we got to the right
    /// branch and we know other branches won't work, so backtrack
    /// as fast as possible
    Failure(String),

    ConfigFileParseErr(String),


    TableSchemaIntoErr(String),
    RcMysqlUrlErr(String),
    RcMysqlQueryErr(String),
    OpRaftErr(String),

    MysqlQueryErr(String),

    OpTableNotExistErr(String),
    OpSchemaNotExistErr(String),
    OpMetadataErr(String),
    MetadataMockErr(String),
}

impl Display for ReError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            ReError::BUG(s) | ReError::Error(s) | ReError::ASTParserError(s)
            | ReError::ConnectionError(s) | ReError::String(s) | ReError::Failure(s)
            | ReError::ConfigFileParseErr(s) | ReError::TableSchemaIntoErr(s) | ReError::RcMysqlUrlErr(s)
            | ReError::RcMysqlQueryErr(s) | ReError::OpRaftErr(s) | ReError::MysqlQueryErr(s)
            | ReError::OpTableNotExistErr(s) | ReError::OpSchemaNotExistErr(s) | ReError::OpMetadataErr(s)
            | ReError::MetadataMockErr(s) => {
                write!(f, "{}", s)
            }
            ReError::Incomplete(n) => {
                write!(f, "{}", n)
            }
            ReError::IoError(err) => {
                write!(f, "{}", err.to_string())
            }
            ReError::Utf8Error(err) => {
                write!(f, "{}", err.to_string())
            }
            ReError::FromUtf8Error(err) => {
                write!(f, "{}", err.to_string())
            }
            ReError::FromHexError(err) => {
                write!(f, "{}", err.to_string())
            }
            ReError::ParseIntError(err) => {
                write!(f, "{}", err.to_string())
            }
        }
    }
}

impl From<io::Error> for ReError {
    fn from(error: io::Error) -> Self {
        ReError::IoError(error)
    }
}

// impl <T> From<error::Error<T>> for ReError {
//     fn  from(error: error::Error<T>) -> Self {
//         ReError::String(error.input)
//     }
// }

impl From<Utf8Error> for ReError {
    fn from(error: Utf8Error) -> Self {
        ReError::Utf8Error(error)
    }
}

impl From<FromUtf8Error> for ReError {
    fn from(error: FromUtf8Error) -> Self {
        ReError::FromUtf8Error(error)
    }
}

impl From<FromHexError> for ReError {
    fn from(error: FromHexError) -> Self {
        ReError::FromHexError(error)
    }
}

impl From<ParseIntError> for ReError {
    fn from(error: ParseIntError) -> Self {
        ReError::ParseIntError(error)
    }
}

/// Contains information on needed data if a parser returned `Incomplete`
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Needed {
    /// Needs more data, but we do not know how much
    Unknown,

    NoEnoughData,

    InvalidUtf8,

    /// 被忽略的异常。
    MissingNull,

    InvalidData(String),
}

impl Display for Needed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            Needed::Unknown => {
                write!(f, "Unknown")
            }
            Needed::NoEnoughData => {
                write!(f, "NoEnoughData")
            }
            Needed::InvalidUtf8 => {
                write!(f, "InvalidUtf8")
            }
            Needed::MissingNull => {
                write!(f, "MissingNull")
            }
            Needed::InvalidData(s) => {
                write!(f, "{}", s)
            }
        }
    }
}

impl ReError {
    pub fn is_error(&self) -> bool {
        print!("a");

        false
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
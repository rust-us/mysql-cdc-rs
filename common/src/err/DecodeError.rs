use std::io;
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use hex::FromHexError;
use nom::error;

#[derive(Debug)]
pub enum ReError {
    IoError(io::Error),
    Utf8Error(Utf8Error),
    FromUtf8Error(FromUtf8Error),
    FromHexError(FromHexError),
    ParseIntError(ParseIntError),
    String(String),

    /// Byte code is incomplete
    Incomplete(Needed),

    /// The parser had an error (recoverable)
    Error(String),

    /// The parser had an unrecoverable error: we got to the right
    /// branch and we know other branches won't work, so backtrack
    /// as fast as possible
    Failure(String),
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

    MissingNull,

    InvalidData(String),
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
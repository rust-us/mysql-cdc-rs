use std::fmt;
use std::fmt::Display;
use serde_derive::{Deserialize, Serialize};

/// Result returning Error
pub type WResult<T> = std::result::Result<T, WebError>;

/// errors. All except Internal are considered user-facing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WebError {
    Abort,
    /// parser 异常
    Parse(String),
    ReadOnly,
    /// 序列化异常
    Serialization(String),
    Value(String),
}

impl std::error::Error for WebError {}

impl Display for WebError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            WebError::Parse(s) | WebError::Value(s) => {
                write!(f, "{}", s)
            }
            WebError::Abort => write!(f, "Operation aborted"),
            WebError::Serialization(s) => write!(f, "Serialization failure, retry transaction"),
            WebError::ReadOnly => write!(f, "Read-only transaction"),
        }
    }
}

impl From<serde_json::error::Error> for WebError {
    fn from(err: serde_json::error::Error) -> Self {
        WebError::Value(err.to_string())
    }
}

impl serde::ser::Error for WebError {
    fn custom<T: Display>(msg: T) -> Self {
        WebError::Serialization(msg.to_string())
    }
}

impl serde::de::Error for WebError {
    fn custom<T: Display>(msg: T) -> Self {
        WebError::Serialization(msg.to_string())
    }
}

impl From<common::err::decode_error::ReError> for WebError {
    fn from(err: common::err::decode_error::ReError) -> Self {
        WebError::Parse(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for WebError {
    fn from(err: std::num::ParseFloatError) -> Self {
        WebError::Parse(err.to_string())
    }
}

impl From<std::num::ParseIntError> for WebError {
    fn from(err: std::num::ParseIntError) -> Self {
        WebError::Parse(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for WebError {
    fn from(err: std::array::TryFromSliceError) -> Self {
        WebError::Value(err.to_string())
    }
}

impl From<std::num::TryFromIntError> for WebError {
    fn from(err: std::num::TryFromIntError) -> Self {
        WebError::Value(err.to_string())
    }
}

impl From<std::io::Error> for WebError {
    fn from(err: std::io::Error) -> Self {
        WebError::Value(err.to_string())
    }
}

impl From<std::net::AddrParseError> for WebError {
    fn from(err: std::net::AddrParseError) -> Self {
        WebError::Value(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for WebError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        WebError::Value(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for WebError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        WebError::Value(err.to_string())
    }
}

impl<T: num_enum::TryFromPrimitive> From<num_enum::TryFromPrimitiveError<T>> for WebError {
    fn from(err: num_enum::TryFromPrimitiveError<T>) -> Self {
        WebError::Value(err.to_string())
    }
}

use std::fmt::Display;
use std::{fmt, io};
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use hex::FromHexError;

/// Enhanced error context for better error reporting and debugging
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub position: Option<u64>,
    pub event_type: Option<u8>,
    pub table_id: Option<u64>,
    pub operation: Option<String>,
    pub additional_info: Option<String>,
}

impl ErrorContext {
    pub fn new() -> Self {
        Self {
            position: None,
            event_type: None,
            table_id: None,
            operation: None,
            additional_info: None,
        }
    }

    pub fn with_position(mut self, position: u64) -> Self {
        self.position = Some(position);
        self
    }

    pub fn with_event_type(mut self, event_type: u8) -> Self {
        self.event_type = Some(event_type);
        self
    }

    pub fn with_table_id(mut self, table_id: u64) -> Self {
        self.table_id = Some(table_id);
        self
    }

    pub fn with_operation(mut self, operation: String) -> Self {
        self.operation = Some(operation);
        self
    }

    pub fn with_info(mut self, info: String) -> Self {
        self.additional_info = Some(info);
        self
    }
}

impl Default for ErrorContext {
    fn default() -> Self {
        Self::new()
    }
}

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
    // Binlog - Enhanced with context
    //////////////////////
    /// Byte code is incomplete
    /// 此错误用于binlog编解码过程中的异常处理，包含：
    ///     编解码进行中、已完成、格式错误等， 由 Needed 产生为具体的错误信息描述
    Incomplete(Needed),

    /// Event parsing error with context
    EventParseError {
        message: String,
        context: ErrorContext,
        source: Option<Box<ReError>>,
    },

    /// Unsupported event type error
    UnsupportedEventType {
        event_type: u8,
        context: ErrorContext,
    },

    /// Invalid data format error
    InvalidDataFormat {
        message: String,
        context: ErrorContext,
        expected_length: Option<usize>,
        actual_length: Option<usize>,
    },

    /// Checksum validation error
    ChecksumMismatch {
        expected: u32,
        actual: u32,
        context: ErrorContext,
    },

    /// Table map not found error
    TableMapNotFound {
        table_id: u64,
        context: ErrorContext,
    },

    /// Memory limit exceeded error
    MemoryLimitExceeded {
        current: usize,
        limit: usize,
        context: ErrorContext,
    },

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
            ReError::EventParseError { message, context, source } => {
                write!(f, "Event parse error: {}", message)?;
                if let Some(pos) = context.position {
                    write!(f, " at position {}", pos)?;
                }
                if let Some(event_type) = context.event_type {
                    write!(f, " (event type: 0x{:02x})", event_type)?;
                }
                if let Some(table_id) = context.table_id {
                    write!(f, " (table id: {})", table_id)?;
                }
                if let Some(op) = &context.operation {
                    write!(f, " during {}", op)?;
                }
                if let Some(info) = &context.additional_info {
                    write!(f, " - {}", info)?;
                }
                if let Some(source_err) = source {
                    write!(f, " caused by: {}", source_err)?;
                }
                Ok(())
            }
            ReError::UnsupportedEventType { event_type, context } => {
                write!(f, "Unsupported event type: 0x{:02x}", event_type)?;
                if let Some(pos) = context.position {
                    write!(f, " at position {}", pos)?;
                }
                Ok(())
            }
            ReError::InvalidDataFormat { message, context, expected_length, actual_length } => {
                write!(f, "Invalid data format: {}", message)?;
                if let Some(expected) = expected_length {
                    if let Some(actual) = actual_length {
                        write!(f, " (expected {} bytes, got {})", expected, actual)?;
                    } else {
                        write!(f, " (expected {} bytes)", expected)?;
                    }
                }
                if let Some(pos) = context.position {
                    write!(f, " at position {}", pos)?;
                }
                Ok(())
            }
            ReError::ChecksumMismatch { expected, actual, context } => {
                write!(f, "Checksum mismatch: expected 0x{:08x}, got 0x{:08x}", expected, actual)?;
                if let Some(pos) = context.position {
                    write!(f, " at position {}", pos)?;
                }
                Ok(())
            }
            ReError::TableMapNotFound { table_id, context } => {
                write!(f, "Table map not found for table id: {}", table_id)?;
                if let Some(pos) = context.position {
                    write!(f, " at position {}", pos)?;
                }
                Ok(())
            }
            ReError::MemoryLimitExceeded { current, limit, context } => {
                write!(f, "Memory limit exceeded: {} > {}", current, limit)?;
                if let Some(pos) = context.position {
                    write!(f, " at position {}", pos)?;
                }
                Ok(())
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
        match self {
            ReError::BUG(_) | ReError::Failure(_) => true,
            ReError::EventParseError { .. } | ReError::UnsupportedEventType { .. } |
            ReError::InvalidDataFormat { .. } | ReError::ChecksumMismatch { .. } |
            ReError::TableMapNotFound { .. } | ReError::MemoryLimitExceeded { .. } => true,
            _ => false,
        }
    }

    /// Check if the error is recoverable (can continue parsing)
    pub fn is_recoverable(&self) -> bool {
        match self {
            ReError::BUG(_) | ReError::Failure(_) | ReError::MemoryLimitExceeded { .. } => false,
            ReError::UnsupportedEventType { .. } | ReError::InvalidDataFormat { .. } => true,
            ReError::EventParseError { .. } | ReError::ChecksumMismatch { .. } => true,
            ReError::TableMapNotFound { .. } => true,
            _ => false,
        }
    }

    /// Create an event parse error with context
    pub fn event_parse_error(message: String, context: ErrorContext) -> Self {
        ReError::EventParseError {
            message,
            context,
            source: None,
        }
    }

    /// Create an event parse error with source error
    pub fn event_parse_error_with_source(message: String, context: ErrorContext, source: ReError) -> Self {
        ReError::EventParseError {
            message,
            context,
            source: Some(Box::new(source)),
        }
    }

    /// Create an unsupported event type error
    pub fn unsupported_event_type(event_type: u8, context: ErrorContext) -> Self {
        ReError::UnsupportedEventType {
            event_type,
            context,
        }
    }

    /// Create an invalid data format error
    pub fn invalid_data_format(message: String, context: ErrorContext) -> Self {
        ReError::InvalidDataFormat {
            message,
            context,
            expected_length: None,
            actual_length: None,
        }
    }

    /// Create an invalid data format error with length information
    pub fn invalid_data_format_with_length(
        message: String,
        context: ErrorContext,
        expected_length: Option<usize>,
        actual_length: Option<usize>,
    ) -> Self {
        ReError::InvalidDataFormat {
            message,
            context,
            expected_length,
            actual_length,
        }
    }

    /// Create a checksum mismatch error
    pub fn checksum_mismatch(expected: u32, actual: u32, context: ErrorContext) -> Self {
        ReError::ChecksumMismatch {
            expected,
            actual,
            context,
        }
    }

    /// Create a table map not found error
    pub fn table_map_not_found(table_id: u64, context: ErrorContext) -> Self {
        ReError::TableMapNotFound {
            table_id,
            context,
        }
    }

    /// Create a memory limit exceeded error
    pub fn memory_limit_exceeded(current: usize, limit: usize, context: ErrorContext) -> Self {
        ReError::MemoryLimitExceeded {
            current,
            limit,
            context,
        }
    }

    /// Get the error context if available
    pub fn get_context(&self) -> Option<&ErrorContext> {
        match self {
            ReError::EventParseError { context, .. } |
            ReError::UnsupportedEventType { context, .. } |
            ReError::InvalidDataFormat { context, .. } |
            ReError::ChecksumMismatch { context, .. } |
            ReError::TableMapNotFound { context, .. } |
            ReError::MemoryLimitExceeded { context, .. } => Some(context),
            _ => None,
        }
    }

    /// Get the position from error context if available
    pub fn get_position(&self) -> Option<u64> {
        self.get_context().and_then(|ctx| ctx.position)
    }

    /// Get the event type from error context if available
    pub fn get_event_type(&self) -> Option<u8> {
        self.get_context().and_then(|ctx| ctx.event_type)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
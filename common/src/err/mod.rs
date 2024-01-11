use crate::err::DecodeError::ReError;

pub mod DecodeError;

pub type CResult<T> = Result<T, ReError>;
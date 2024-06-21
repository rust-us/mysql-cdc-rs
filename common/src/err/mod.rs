use crate::err::decode_error::ReError;

pub mod decode_error;

pub type CResult<T> = Result<T, ReError>;
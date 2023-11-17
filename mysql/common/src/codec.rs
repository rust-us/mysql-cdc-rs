use thiserror::Error;

/// 编码错误
#[derive(Debug, Clone, Error)]
pub enum DecodeError {
    #[error("no enough data")]
    NoEnoughData,

    #[error("invalid utf-8 string")]
    InvalidUtf8,

    #[error("missing terminal null bytes")]
    MissingNull,

    #[error("invalid data")]
    InvalidData,
}


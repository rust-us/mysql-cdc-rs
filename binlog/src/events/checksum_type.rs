use serde::Serialize;
use common::err::DecodeError::ReError;

/// checksum_alg size， 1 byte
pub const BINLOG_CHECKSUM_ALG_DESC_LEN: u8 = 1;

/// checksum size，4 byte
pub const ST_COMMON_PAYLOAD_CHECKSUM_LEN: u8 = 4;

/// Checksum type used in a binlog file.
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum ChecksumType {
    /// Checksum is disabled.
    None = 0,

    /// CRC32 checksum.
    Crc32 = 1,
}

impl ChecksumType {
    pub fn from_code(code: u8) -> Result<Self, ReError> {
        match code {
            0 => Ok(ChecksumType::None),
            1 => Ok(ChecksumType::Crc32),
            _ => Err(ReError::String(
                format!("The master checksum type is not supported: {}", code).to_string(),
            )),
        }
    }

    pub fn from_name(name: &str) -> Result<Self, ReError> {
        match name {
            "NONE" => Ok(ChecksumType::None),
            "CRC32" => Ok(ChecksumType::Crc32),
            _ => Err(ReError::String(
                format!("The master checksum type is not supported: {}", name).to_string(),
            )),
        }
    }
}

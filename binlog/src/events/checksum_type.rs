use serde::Serialize;
use common::err::DecodeError::ReError;

/// checksum_alg size， 1 byte
pub const BINLOG_CHECKSUM_ALG_DESC_LEN: u8 = 1;

/// checksum size，4 byte
pub const ST_COMMON_PAYLOAD_CHECKSUM_LEN: u8 = 4;

/// Events are without checksum though its generator
pub const BINLOG_CHECKSUM_ALG_OFF: u8 = 0;
/// is checksum-capable New Master (NM). CRC32 of zlib algorithm.
pub const BINLOG_CHECKSUM_ALG_CRC32: u8 = 1;
/// the cut line: valid alg range is [1, 0x7f].
pub const BINLOG_CHECKSUM_ALG_ENUM_END: u8 = 2;
/// special value to tag undetermined yet checksum
pub const BINLOG_CHECKSUM_ALG_UNDEF: u8 = 255;


#[repr(u8)] /// Checksum type used in a binlog file.
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum ChecksumType {
    /// Checksum is disabled.
    None = BINLOG_CHECKSUM_ALG_OFF,

    /// CRC32 checksum.
    Crc32 = BINLOG_CHECKSUM_ALG_CRC32,
}

impl ChecksumType {
    pub fn from_code(code: u8) -> Result<Self, ReError> {
        match code {
            BINLOG_CHECKSUM_ALG_OFF => Ok(ChecksumType::None),
            BINLOG_CHECKSUM_ALG_CRC32 => Ok(ChecksumType::Crc32),
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

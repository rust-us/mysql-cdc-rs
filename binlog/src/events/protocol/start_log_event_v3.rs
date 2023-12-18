use serde::Serialize;
use crate::events::log_event::LogEvent;

/// We could have used SERVER_VERSION_LENGTH, but this introduces an obscure
/// dependency - if somebody decided to change SERVER_VERSION_LENGTH this
/// would break the replication protocol
pub const ST_SERVER_VER_LEN: u8 = 50;

/// start event post-header (for v3 and v4)
pub const ST_BINLOG_VER_OFFSET: u8 = 0;

pub const ST_SERVER_VER_OFFSET: u8 = 2;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct StartLogEventV3 {
    binlog_version: u32,

    server_version: String,
}

impl LogEvent for StartLogEventV3 {

}
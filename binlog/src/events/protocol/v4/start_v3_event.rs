use std::collections::HashMap;
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::events::declare::log_event::LogEvent;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::protocol::table_map_event::TableMapEvent;

/// We could have used SERVER_VERSION_LENGTH, but this introduces an obscure
/// dependency - if somebody decided to change SERVER_VERSION_LENGTH this
/// would break the replication protocol
pub const ST_SERVER_VER_LEN: u8 = 50;

/// start event post-header (for v3 and v4)
pub const ST_BINLOG_VER_OFFSET: u8 = 0;

pub const ST_SERVER_VER_OFFSET: u8 = 2;

#[derive(Debug, Serialize, Clone)]
pub struct StartV3Event {
    header: Header,

    binlog_version: u16,
    server_version: String,
}

impl StartV3Event {
    pub fn new(header: Header, binlog_version: u16, server_version: &str) -> Self {
        StartV3Event {
            header,
            binlog_version,
            server_version: server_version.to_string(),
        }
    }
}

impl LogEvent for StartV3Event {
    fn get_type_name(&self) -> String {
        "StartV3Event".to_string()
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
    ) -> Result<Self, ReError> {

        let _context = context.borrow_mut();
        let common_header_len = _context.get_format_description().common_header_len;

        let binlog_version = cursor.read_u16::<LittleEndian>().unwrap();
        let mut server_version = String::new();
        cursor.read_to_string(&mut server_version).unwrap();

        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        Ok(StartV3Event {
            header: Header::copy(header),
            binlog_version,
            server_version
        })
    }
}


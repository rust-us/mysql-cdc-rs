use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::rc::Rc;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::events::checksum_type::ChecksumType;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext};
use crate::events::log_event::LogEvent;

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

    pub fn parse(cursor: &mut Cursor<&[u8]>, header: &Header,
                 context: Rc<RefCell<LogContext>>, checksum_type: &ChecksumType) -> Result<StartV3Event, ReError> {

        let _context = context.borrow_mut();
        let common_header_len = _context.get_format_description().common_header_len;

        let binlog_version = cursor.read_u16::<LittleEndian>().unwrap();
        let mut server_version = String::new();
        cursor.read_to_string(&mut server_version).unwrap();

        let checksum = cursor.read_u32::<LittleEndian>()?;

        Ok(StartV3Event {
            header: Header::copy_and_get(header, checksum, HashMap::new()),
            binlog_version,
            server_version
        })
    }
}

impl LogEvent for StartV3Event {
    fn get_type_name(&self) -> String {
        "StartV3Event".to_string()
    }
}


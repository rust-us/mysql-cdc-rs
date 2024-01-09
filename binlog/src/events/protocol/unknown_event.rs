use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::events::checksum_type::ChecksumType;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::log_event::LogEvent;

#[derive(Debug, Serialize, Clone)]
pub struct UnknownEvent {
    header: Header,
}

impl UnknownEvent {

    pub fn new(header: Header) -> Self {
        UnknownEvent {
            header
        }
    }

    pub fn parse(cursor: &mut Cursor<&[u8]>, header: &Header,
        context: Rc<RefCell<LogContext>>, checksum_type: &ChecksumType) -> Result<UnknownEvent, ReError> {

        let checksum = cursor.read_u32::<LittleEndian>()?;

        Ok(UnknownEvent { header: Header::copy_and_get(header, checksum, HashMap::new()), })
    }
}

impl LogEvent for UnknownEvent {
    fn get_type_name(&self) -> String {
        "UnknownEvent".to_string()
    }
}

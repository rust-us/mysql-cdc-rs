use std::collections::HashMap;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct SlaveEvent {
    header: Header,

}

impl LogEvent for SlaveEvent {
    fn get_type_name(&self) -> String {
        "SlaveEvent".to_string()
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
    ) -> Result<SlaveEvent, ReError> where Self: Sized {
        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        Ok(SlaveEvent {
            header: Header::copy(header.clone()),
        })
    }

}
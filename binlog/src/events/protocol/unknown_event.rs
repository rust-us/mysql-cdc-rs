use std::collections::HashMap;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::{LogContextRef};
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

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

}

impl LogEvent for UnknownEvent {
    fn get_type_name(&self) -> String {
        "UnknownEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(cursor: &mut Cursor<&[u8]>, header: HeaderRef, context: LogContextRef, table_map: Option<&HashMap<u64, TableMapEvent>>,
             table_cache_manager: Option<&TableCacheManager>,)
        -> Result<UnknownEvent, ReError> {
        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        Ok(UnknownEvent { header: Header::copy(header), })
    }
}

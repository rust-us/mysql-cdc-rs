use std::collections::HashMap;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::declare::log_event::LogEvent;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct XidLogEvent {
    header: Header,

    pub xid: u64,
}

impl XidLogEvent {
    pub fn new(header: Header, xid: u64) -> XidLogEvent {
        XidLogEvent {
            header,
            xid,
        }
    }

    pub fn get_xid(&self) -> u64 {
        self.xid
    }
}

impl LogEvent for XidLogEvent {
    fn get_type_name(&self) -> String {
        "XidLogEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<XidLogEvent, ReError> where Self: Sized {
        let xid = cursor.read_u64::<LittleEndian>()?;

        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);

        Ok(XidLogEvent::new(Header::copy(header.clone()), xid))
    }
}
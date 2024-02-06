use std::collections::HashMap;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct StopEvent {
    header: Header,

}

impl StopEvent {
    pub fn new(header: Header) -> StopEvent {
        StopEvent {
            header,
        }
    }
}

impl LogEvent for StopEvent {
    fn get_type_name(&self) -> String {
        "StopEvent".to_string()
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
    ) -> Result<StopEvent, ReError> where Self: Sized {
        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        Ok(StopEvent {
            header: Header::copy(header.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}

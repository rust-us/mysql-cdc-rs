use std::collections::HashMap;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::{Needed, ReError};
use crate::events::declare::log_event::LogEvent;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct IntVarEvent {
    header: Header,

    pub e_type: IntVarEventType,

    pub value: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum IntVarEventType {
    /// 0x00
    InvalidIntEvent,
    /// 0x01
    LastInsertIdEvent,
    /// 0x02
    InsertIdEvent,
}

impl LogEvent for IntVarEvent {
    fn get_type_name(&self) -> String {
        "IntVarEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
    ) -> Result<Self, ReError> where Self: Sized {
        let t = cursor.read_u8()?;
        let e_type = match t {
            0x00 => Ok(IntVarEventType::InvalidIntEvent),
            0x01 => Ok(IntVarEventType::LastInsertIdEvent),
            0x02 => Ok(IntVarEventType::InsertIdEvent),
            _ => Err(ReError::Incomplete(Needed::InvalidData(
                format!("parser IntVar type error, type: {}", t)
            ))),
        }.unwrap();

        let value = cursor.read_u64::<LittleEndian>()?;
        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        Ok(IntVarEvent {
            header: Header::copy(header),
            e_type,
            value,
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

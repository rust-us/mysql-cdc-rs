use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use crate::events::event_header::Header;
use crate::events::declare::log_event::LogEvent;
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::alias::mysql::gtid::gtid_set::GtidSet;
use crate::alias::mysql::gtid::interval::Interval;
use crate::alias::mysql::gtid::uuid::Uuid;
use crate::alias::mysql::gtid::uuid_set::UuidSet;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;

/// source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L1073-L1103
#[derive(Debug, Serialize, Clone)]
pub struct PreviousGtidsLogEvent {
    header: Header,

    /// Gets GtidSet of previous files.,  It contains the Gtids executed in the last binary log file.
    pub gtid_sets: GtidSet,
}

impl Default for PreviousGtidsLogEvent {
    fn default() -> Self {
        PreviousGtidsLogEvent {
            header: Default::default(),
            gtid_sets: GtidSet { uuid_sets: BTreeMap::new() },
        }
    }
}

impl PreviousGtidsLogEvent {
    pub fn new(header: Header, gtid_sets: GtidSet) -> Self {
        PreviousGtidsLogEvent {
            header,
            gtid_sets
        }
    }
}

impl LogEvent for PreviousGtidsLogEvent {
    fn get_type_name(&self) -> String {
        "PreviousGtidsLogEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(cursor: &mut Cursor<&[u8]>, header: HeaderRef,
             context: LogContextRef, table_map: Option<&HashMap<u64, TableMapEvent>>) -> Result<Self, ReError> where Self: Sized {
        let uuid_set_number = cursor.read_u64::<LittleEndian>()?;

        //     let gtid_sets_len = header.borrow().event_length
        //         - (LOG_EVENT_MINIMAL_HEADER_LEN + /*buf_size len*/4 + /*checksum len*/ST_COMMON_PAYLOAD_CHECKSUM_LEN)
        //             as u32;
        //     let (i, gtid_sets) = map(take(gtid_sets_len), |s: &[u8]| s.to_vec())(input)?;
        //
        //     let (i, buf_size) = le_u32(i)?;
        //
        //     let (i, checksum) = le_u32(i)?;

        let mut gtid_set = GtidSet::new();
        for _i in 0..uuid_set_number {
            let mut source_id = [0u8; 16];
            cursor.read_exact(&mut source_id)?;
            let source_id = Uuid::new(source_id);

            let mut uuid_set = UuidSet::new(source_id, Vec::new());
            let interval_number = cursor.read_u64::<LittleEndian>()?;
            for _y in 0..interval_number {
                let start = cursor.read_u64::<LittleEndian>()?;
                let end = cursor.read_u64::<LittleEndian>()?;
                uuid_set.intervals.push(Interval::new(start, end - 1));
            }
            gtid_set
                .uuid_sets
                .insert(uuid_set.source_id.uuid.clone(), uuid_set);
        }

        let checksum = cursor.read_u32::<LittleEndian>()?;
        header.borrow_mut().update_checksum(checksum);

        Ok(PreviousGtidsLogEvent::new(Header::copy(header.clone()), gtid_set),)
    }
}

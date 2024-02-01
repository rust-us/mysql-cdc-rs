use std::collections::HashMap;
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use crate::events::event_header::Header;
use serde::Serialize;
use common::err::CResult;
use common::err::decode_error::ReError;
use crate::alias::mysql::gtid::gtid::Gtid;
use crate::alias::mysql::gtid::uuid::Uuid;
use crate::events::declare::log_event::LogEvent;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;

pub const LOGICAL_TIMESTAMP_TYPE_CODE: u8 = 2;

/// https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L1048-L1056
/// (equals AnonymousGtidLogEvent)
#[derive(Debug, Serialize, Clone)]
pub struct GtidLogEvent {
    header: Header,

    /// 记录binlog格式
    /// 如果gtid_flags值为1，表示binlog中可能有以statement方式记录的binlog。 此时 commit_flag 为 true
    /// 如果为0表示，binlog中只有以row格式记录的binlog。 此时 commit_flag 为 false
    pub commit_flag: u8,

    /// Gets Global Transaction ID of the event group.
    pub gtid: Gtid,

    /// logical_timestamp_typecode
    pub lt_type: u8,

    pub last_committed: i64,
    pub sequence_number: i64,
}

impl GtidLogEvent {
    pub fn new(
        header: Header,
        flags: u8,
        gtid: Gtid,
        lt_type: u8,
        last_committed: i64,
        sequence_number: i64,
    ) -> Self {
        GtidLogEvent {
            header,
            commit_flag: flags,
            gtid,
            lt_type,
            last_committed,
            sequence_number,
        }
    }

    pub fn parse_events_gtid(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
    ) -> CResult<(u8, Uuid, u64, u8, i64, i64, u32)> {
        // 记录binlog格式:
        // 如果gtid_flags值为1，表示binlog中可能有以statement方式记录的binlog
        // 如果为0表示，binlog中只有以row格式记录的binlog
        let flags = cursor.read_u8()?;

        // let (i, high) = be_u64(i)?;
        // let (i, low) = be_u64(i)?;
        // let mut source_id_uuid = Uuid::default();
        // if high == 0 && low == 0 {
        //     source_id_uuid = Uuid::default();
        // } else {
        //     source_id_uuid = Uuid::from_u64_pair(high, low);
        // }
        // let source_id = source_id_uuid.to_string();
        let mut source_id = [0u8; 16];
        cursor.read_exact(&mut source_id)?;
        let source_id: Uuid = Uuid::new(source_id);

        let transaction_id = cursor.read_u64::<LittleEndian>()?;

        let lt_type = cursor.read_u8()?;

        let (last_committed, sequence_number, checksum) = match (lt_type == LOGICAL_TIMESTAMP_TYPE_CODE) {
            true => {
                let last_committed = cursor.read_i64::<LittleEndian>()?;
                let sequence_number = cursor.read_i64::<LittleEndian>()?;

                let remain_len = header.borrow().get_event_length() - (19 + 1 + 16 + 8 + 1 + 8 + 8);
                if remain_len > 4 {
                    let mut _s = vec![0; (remain_len - 4) as usize];
                    cursor.read_exact(&mut _s)?;

                    let checksum = cursor.read_u32::<LittleEndian>()?;

                    (last_committed, sequence_number, checksum)
                } else {
                    let checksum = cursor.read_u32::<LittleEndian>()?;

                    (last_committed, sequence_number, checksum)
                }
            },
            false=> {
                let checksum = cursor.read_u32::<LittleEndian>()?;

                (0, 0, checksum)
            },
        };

        Ok((flags,
            source_id,
            transaction_id,
            lt_type,
            last_committed,
            sequence_number,
            checksum,
        ))
    }

    pub fn get_last_committed(&self) -> i64 {
        self.last_committed
    }

    pub fn get_sequence_number(&self) -> i64 {
        self.sequence_number
    }

    pub fn get_gtid_str(&self) -> String {
        format!("{}", self.gtid.to_string())
    }
}

impl LogEvent for GtidLogEvent {
    fn get_type_name(&self) -> String {
        "GtidLogEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(cursor: &mut Cursor<&[u8]>,
             header: HeaderRef, context: LogContextRef,
             table_map: Option<&HashMap<u64, TableMapEvent>>) -> Result<Self, ReError> where Self: Sized {
        let (
            flags,
            source_id,
            transaction_id,
            lt_type,
            last_committed,
            sequence_number,
            checksum,
        ) = GtidLogEvent::parse_events_gtid(cursor, header.clone()).unwrap();

        header.borrow_mut().update_checksum(checksum);

        let gtid = Gtid::new(source_id, transaction_id);

        let e = GtidLogEvent {
            header: Header::copy(header),
            commit_flag: flags,
            gtid,
            lt_type,
            last_committed,
            sequence_number,
        };

        Ok(e)
    }
}

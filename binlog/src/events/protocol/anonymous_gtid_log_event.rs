use std::collections::HashMap;
use std::io::Cursor;
use crate::events::event_header::Header;
use crate::events::declare::log_event::LogEvent;
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::alias::mysql::events::gtid_log_event::GtidLogEvent;
use crate::alias::mysql::gtid::gtid::Gtid;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;

/// MySQL在binlog中记录每一个匿名事务之前会记录一个Anonymous_gtid_log_event表示接下来的事务是一个匿名事务。
/// 注意：因为在5.6.34中并不会产生Anonymous_gtid_log_event，5.7.19版本才有.
///
/// Anonymous_gtid_log_event格式(equals GtidLogEvent)
/// +=====================================+============================+============================+
/// |        | 字段          | 字节数   |            描述             |
/// +=====================================+============================+============================+
/// | post   | gtid_flags/commit_flag   | 1字节    | 记录binlog格式，如果gtid_flags值为1，表示binlog中可能有以statement方式记录的binlog，如果为0表示，binlog中只有以row格式记录的binlog    |
/// | header +----------------------------+----------------------------+----------------------------+
/// |        | sid          | 16字节   | 记录GTID中uuid的部分（不记录‘-’），每1个字节表示uuid中2个字符       |
/// |        +----------------------------+----------------------------+----------------------------+
/// |        | gno          | 8字节    | 小端存储，GTID中的事务号部分                                     |
/// |        +----------------------------+----------------------------+----------------------------+
/// |        | logical_timestamp_typecode    | 1字节   | 判断是否有last_commit和sequence_no，在logical_tmiestamp_typecode=2的情况下，有last_commit和sequence_no    |
/// |        +----------------------------+----------------------------+----------------------------+
/// |        | last_commit    | 8字节   | 小端存储，上次提交的事务号                                      |
/// |        +----------------------------+----------------------------+----------------------------+
/// |        | sequence_no   | 8字节    | 小端存储，本次提交的序列号                                      |
/// +=====================================+============================+============================+
#[derive(Debug, Serialize, Clone)]
pub struct AnonymousGtidLogEvent {
    pub gtid_event: GtidLogEvent
}

impl LogEvent for AnonymousGtidLogEvent {
    fn get_type_name(&self) -> String {
        "AnonymousGtidLog".to_string()
    }

    fn len(&self) -> i32 {
        self.gtid_event.len()
    }

    fn parse(cursor: &mut Cursor<&[u8]>, header: HeaderRef, context: LogContextRef,
             table_map: Option<&HashMap<u64, TableMapEvent>>,
             table_cache_manager: Option<&TableCacheManager>,) -> Result<AnonymousGtidLogEvent, ReError> where Self: Sized {
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

        let gtid_event = GtidLogEvent::new(Header::copy(header), flags, gtid, lt_type, last_committed, sequence_number);

        Ok(AnonymousGtidLogEvent {
            gtid_event
        })
    }
}

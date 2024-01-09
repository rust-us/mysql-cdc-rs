use std::collections::HashMap;
use crate::events::event::Event;
use crate::events::event_header::Header;
use crate::events::log_event::LogEvent;
use crate::events::protocol::gtid_log_event::GtidLogEvent;
use nom::combinator::map;
use nom::IResult;
use serde::Serialize;
use std::rc::Rc;

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
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct AnonymousGtidLogEvent {
    header: Header,

    /// 记录binlog格式
    /// 如果gtid_flags值为1，表示binlog中可能有以statement方式记录的binlog。 此时 commit_flag 为 true
    /// 如果为0表示，binlog中只有以row格式记录的binlog。 此时 commit_flag 为 false
    pub commit_flag: bool,

    /// 16字节
    pub sid: String,
    /// 8字节, transaction_id
    pub gno: String,

    /// logical_timestamp_typecode
    pub lt_type: u8,

    pub last_committed: i64,
    pub sequence_number: i64,
}

impl AnonymousGtidLogEvent {
    pub fn parse<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], AnonymousGtidLogEvent> {
        let (
            i,
            (
                commit_flag,
                source_id,
                transaction_id,
                lt_type,
                last_committed,
                sequence_number,
                checksum,
            ),
        ) = GtidLogEvent::parse_events_gtid(input, &header)?;

        let header_new = Header::copy_and_get(&header, checksum, HashMap::new());

        let e = AnonymousGtidLogEvent {
            header: header_new,
            commit_flag,
            sid: source_id,
            gno: transaction_id,
            lt_type,
            last_committed,
            sequence_number,
        };

        Ok((i, e))
    }
}

impl LogEvent for AnonymousGtidLogEvent {
    fn get_type_name(&self) -> String {
        "AnonymousGtidLog".to_string()
    }
}

use crate::events::event::Event;

/// 3 is MySQL 4.x; 4 is MySQL 5.0.0. Compared to version 3, version 4 has: -
/// a different Start_log_event, which includes info about the binary log
/// (sizes of headers); this info is included for better compatibility if the
/// master's MySQL version is different from the slave's. - all events have a
/// unique ID (the triplet (server_id, timestamp at server start, other) to
/// be sure an event is not executed more than once in a multimaster setup,
/// example: M1 / \ v v M2 M3 \ / v v S if a query is run on M1, it will
/// arrive twice on S, so we need that S remembers the last unique ID it has
/// processed, to compare and know if the event should be skipped or not.
/// Example of ID: we already have the server id (4 bytes), plus:
/// timestamp_when_the_master_started (4 bytes), a counter (a sequence number
/// which increments every time we write an event to the binlog) (3 bytes).
/// Q: how do we handle when the counter is overflowed and restarts from 0 ?
/// - Query and Load (Create or Execute) events may have a more precise
/// timestamp (with microseconds), number of matched/affected/warnings rows
/// and fields of session variables: SQL_MODE, FOREIGN_KEY_CHECKS,
/// UNIQUE_CHECKS, SQL_AUTO_IS_NULL, the collations and charsets, the
/// PASSWORD() version (old/new/...).
pub const BINLOG_VERSION: i32 = 4;

/// Default 5.0 server version
pub const SERVER_VERSION: &str = "5.0";
pub const SERVER_VERSION_4: &str = "4.0";
pub const SERVER_VERSION_3: &str = "3.23";

////////////////////////////////////////////////////////////////////////
/// Event header offsets; these point to places inside the fixed header.
////////////////////////////////////////////////////////////////////////
pub const EVENT_TYPE_OFFSEN: i32 = 4;
pub const SERVER_ID_OFFSEN: i32 = 5;
pub const EVENT_LEN_OFFSETN: i32 = 9;
pub const LOG_POS_OFFSETN: i32 = 13;
pub const FLAGS_OFFSETN: i32 = 17;

///
/// 1 byte length, 1 byte format Length is total length in bytes, including 2
/// byte header Length values 0 and 1 are currently invalid and reserved.
///
pub const EXTRA_ROW_INFO_LEN_OFFSET: u8 = 0;
pub const EXTRA_ROW_INFO_FORMAT_OFFSET: u8 = 1;
pub const EXTRA_ROW_INFO_HDR_BYTES: u8 = 2;
pub const EXTRA_ROW_INFO_MAX_PAYLOAD: u8 = (255 - EXTRA_ROW_INFO_HDR_BYTES);

/// event-specific post-header sizes where 3.23, 4.x and 5.0 agree.
/// 11 byte
pub const QUERY_HEADER_MINIMAL_LEN: u8 = (4 + 4 + 1 + 2);
/// where 5.0 differs: 2 for len of N-bytes vars.
/// 13 byte
pub const QUERY_HEADER_LEN: u8 = (QUERY_HEADER_MINIMAL_LEN + 2);

pub trait LogEvent {
    /// 事件名
    fn get_type_name(&self) -> String;
}

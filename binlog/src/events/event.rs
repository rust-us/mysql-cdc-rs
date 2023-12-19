use crate::{
    mysql::{ColTypes, ColValues},
};
use crate::events::{DupHandlingFlags, EmptyFlags, IncidentEventType, IntVarEventType, OptFlags, query, rows, UserVarType};
use crate::events::event_header::Header;

use serde::Serialize;
use nom::{InputLength, IResult, Parser};
use std::cell::RefCell;
use std::rc::Rc;
use bytes::Buf;
use crate::decoder::event_decoder::LogEventDecoder;
use crate::events::log_context::LogContext;
use crate::events::protocol::anonymous_gtid_log_event::AnonymousGtidLogEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::gtid_log_event::GtidLogEvent;
use crate::events::protocol::previous_gtids_event::PreviousGtidsLogEvent;
use crate::events::protocol::query_event::QueryEvent;

///
/// Enumeration type for the different types of log events.
///
/// @see  https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html
///
/// enum def:
/// enum xxxbEvent {
///     PageLoad,
///     PageUnload,
///     // 1. 普通的结构体
///     Click { x: i64, y: i64 }
///     // 2. 元组结构体
///     KeyPress(char),
///     Paste(String),
/// }
///
/// will be instand LogEvent
#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum Event {
    /// 0
    /// ref: https://dev.mysql.com/doc/internals/en/ignored-events.html#unknown-event
    Unknown {
        header: Header,
        checksum: u32,
    },
    /// 1
    /// 事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
    StartV3,

    /// 2
    Query(QueryEvent),
    /// 3
    /// ref: https://dev.mysql.com/doc/internals/en/stop-event.html
    Stop {
        header: Header,
        checksum: u32,
    },
    /// 4
    /// ref: https://dev.mysql.com/doc/internals/en/rotate-event.html
    Rotate {
        header: Header,
        position: u64,
        next_binlog: String,
        checksum: u32,
    },
    /// 5
    /// ref: https://dev.mysql.com/doc/internals/en/intvar-event.html
    IntVar {
        header: Header,
        e_type: IntVarEventType,
        value: u64,
        checksum: u32,
    },
    /// 6
    /// ref: https://dev.mysql.com/doc/internals/en/load-event.html
    Load {
        header: Header,
        thread_id: u32,
        execution_time: u32,
        skip_lines: u32,
        table_name_length: u8,
        schema_length: u8,
        num_fields: u32,
        field_term: u8,
        enclosed_by: u8,
        line_term: u8,
        line_start: u8,
        escaped_by: u8,
        opt_flags: OptFlags,
        empty_flags: EmptyFlags,
        field_name_lengths: Vec<u8>,
        field_names: Vec<String>,
        table_name: String,
        schema_name: String,
        file_name: String,
        checksum: u32,
    },
    /// 7
    /// ref: https://dev.mysql.com/doc/internals/en/ignored-events.html#slave-event
    Slave {
        header: Header,
        checksum: u32,
    },
    /// 8
    /// ref: https://dev.mysql.com/doc/internals/en/create-file-event.html
    CreateFile {
        header: Header,
        file_id: u32,
        block_data: String,
        checksum: u32,
    },
    /// 9
    /// ref: https://dev.mysql.com/doc/internals/en/append-block-event.html
    AppendBlock {
        header: Header,
        file_id: u32,
        block_data: String,
        checksum: u32,
    },
    /// 10
    /// ref: https://dev.mysql.com/doc/internals/en/exec-load-event.html
    ExecLoad {
        header: Header,
        file_id: u16,
        checksum: u32,
    },
    /// 11
    /// ref: https://dev.mysql.com/doc/internals/en/delete-file-event.html
    DeleteFile {
        header: Header,
        file_id: u16,
        checksum: u32,
    },
    /// 12
    /// ref: https://dev.mysql.com/doc/internals/en/new-load-event.html
    NewLoad {
        header: Header,
        thread_id: u32,
        execution_time: u32,
        skip_lines: u32,
        table_name_length: u8,
        schema_length: u8,
        num_fields: u32,

        field_term_length: u8,
        field_term: String,
        enclosed_by_length: u8,
        enclosed_by: String,
        line_term_length: u8,
        line_term: String,
        line_start_length: u8,
        line_start: String,
        escaped_by_length: u8,
        escaped_by: String,
        opt_flags: OptFlags,

        field_name_lengths: Vec<u8>,
        field_names: Vec<String>,
        table_name: String,
        schema_name: String,
        file_name: String,
        checksum: u32,
    },
    /// 13
    /// ref: https://dev.mysql.com/doc/internals/en/rand-event.html
    Rand {
        header: Header,
        seed1: u64,
        seed2: u64,
        checksum: u32,
    },
    /// 14
    /// ref: https://dev.mysql.com/doc/internals/en/user-var-event.html
    /// source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/statement_events.h#L712-L779
    /// NOTE ref is broken !!!
    UserVar {
        header: Header,
        name_length: u32,
        name: String,
        is_null: bool,
        d_type: Option<UserVarType>,
        charset: Option<u32>,
        value_length: Option<u32>,
        value: Option<Vec<u8>>,
        flags: Option<u8>,
        checksum: u32,
    },
    /// 15
    FormatDescription(FormatDescriptionEvent),
    /// 16
    XID {
        header: Header,
        xid: u64,
        checksum: u32,
    },
    /// 17
    /// ref: https://dev.mysql.com/doc/internals/en/begin-load-query-event.html
    BeginLoadQuery {
        header: Header,
        file_id: u32,
        block_data: String,
        checksum: u32,
    },
    /// 18
    ExecuteLoadQueryEvent {
        header: Header,
        thread_id: u32,
        execution_time: u32,
        schema_length: u8,
        error_code: u16,
        status_vars_length: u16,
        file_id: u32,
        start_pos: u32,
        end_pos: u32,
        dup_handling_flags: DupHandlingFlags,
        status_vars: Vec<query::QueryStatusVar>,
        schema: String,
        query: String,
        checksum: u32,
    },
    /// 19
    TableMap {
        header: Header,
        // table_id take 6 bytes in buffer
        table_id: u64,
        flags: u16,

        schema_length: u8,
        schema: String,

        table_name_length: u8,
        table_name: String,

        columns_number: u64,

        // column_types
        column_metadata: Vec<ColTypes>,
        null_bits: Vec<u8>,
        checksum: u32,
    },

    ///These event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
    /// 20
    PreGaWriteRowsEvent,
    /// 21
    PreGaUpdateRowsEven,
    /// 22
    PreGaDeleteRowsEven,

    /// These event numbers are used from 5.1.16 and forward
    /// The V1 event numbers are used from 5.1.16 until mysql-5.6.
    /// 23, 24, 25
    WRITE_ROWS_V1,
    UPDATE_ROWS_V1,
    DELETE_ROWS_V1,

    /// 26
    /// Something out of the ordinary happened on the master.
    /// ref: https://dev.mysql.com/doc/internals/en/incident-event.html
    Incident {
        header: Header,
        d_type: IncidentEventType,
        message_length: u8,
        message: String,
        checksum: u32,
    },
    /// 27
    /// Heartbeat event to be send by master at its idle time to ensure master's online status to slave.
    /// ref: https://dev.mysql.com/doc/internals/en/heartbeat-event.html
    Heartbeat {
        header: Header,
        checksum: u32,
    },

    /// 28
    /// In some situations, it is necessary to send over ignorable data to the
    /// slave: data that a slave can handle in case there is code for handling
    /// it, but which can be ignored if it is not recognized.
    IgnorableLogEvent,

    /// 29
    /// ref: https://dev.mysql.com/doc/internals/en/rows-query-event.html
    RowQuery {
        header: Header,
        length: u8,
        query_text: String,
        checksum: u32,
    },

    /// Version 2 of the Row events
    /// 30
    /// source https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/rows_event.h#L488-L613
    WriteRowsV2 {
        header: Header,
        /// Post-Header for Rows_event
        // table_id take 6 bytes in buffer.  The number that identifies the table
        table_id: u64,
        // 2 byte bitfield. Reserved for future use; currently always 0.
        // 如：
        // flags:
        //     end_of_stmt: true
        //     foreign_key_checks: true
        //     unique_key_checks: true
        //     has_columns: true
        flags: rows::Flags,
        extra_data_len: u16,
        extra_data: Vec<rows::ExtraData>,
        column_count: u64,
        inserted_image_bits: Vec<u8>,
        rows: Vec<Vec<ColValues>>,
        checksum: u32,
    },
    /// 31
    UpdateRowsV2 {
        header: Header,
        // table_id take 6 bytes in buffer
        table_id: u64,
        flags: rows::Flags,
        extra_data_len: u16,
        extra_data: Vec<rows::ExtraData>,
        column_count: u64,
        before_image_bits: Vec<u8>,
        after_image_bits: Vec<u8>,
        rows: Vec<Vec<ColValues>>,
        checksum: u32,
    },
    /// 32
    DeleteRowsV2 {
        header: Header,
        // table_id take 6 bytes in buffer
        table_id: u64,
        flags: rows::Flags,
        extra_data_len: u16,
        extra_data: Vec<rows::ExtraData>,
        column_count: u64,
        deleted_image_bits: Vec<u8>,
        rows: Vec<Vec<ColValues>>,
        checksum: u32,
    },

    /// 33
    /// equals AnonymousGtidLog
    GtidLog(GtidLogEvent),
    /// 34
    /// equals GtidLog
    AnonymousGtidLog(AnonymousGtidLogEvent),
    /// 35
    PreviousGtidsLog(PreviousGtidsLogEvent),

    /// MySQL 5.7 events
    /// 36
    TRANSACTION_CONTEXT,
    /// 37
    VIEW_CHANGE,

    /// 38
    /// Prepared XA transaction terminal event similar to Xid
    XA_PREPARE_LOG,

    /// 39
    /// Extension of UPDATE_ROWS_EVENT, allowing partial values according to binlog_row_value_options.
    PARTIAL_UPDATE_ROWS,

    /// mysql 8.0.20
    /// 40
    TRANSACTION_PAYLOAD,

    /// mysql 8.0.26
    /// 41
    /// @see https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html#a4a991abea842d4e50cbee0e490c28ceea1b1312ed0f5322b720ab2b957b0e9999
    HEARTBEAT_LOG_V2,

    /// 42
    MYSQL_ENUM_END,

    /** end marker */
    /// Add new events here - right above this comment! Existing events (except ENUM_END_EVENT) should never change their numbers.
    ENUM_END_EVENT
}

impl Event {
    /// 接口作废
    /// event数据结构:         [startPos : Len]
    /// +=====================================+
    /// | event  | timestamp         0 : 4    |
    /// | header +----------------------------+
    /// |        | event_type         4 : 1    |
    /// |        +----------------------------+
    /// |        | server_id         5 : 4    |
    /// |        +----------------------------+
    /// |        | event_size        9 : 4    |
    /// |        +----------------------------+
    /// |        | next_position    13 : 4    |
    /// |        +----------------------------+
    /// |        | flags            17 : 2    |
    /// |        +----------------------------+
    /// |        | extra_headers    19 : x-19 |
    /// +=====================================+
    /// | event  | fixed part        x : y    |
    /// | data   +----------------------------+
    /// |        | variable part              |
    /// +=====================================+
    /// 接口作废
    pub fn parse<'a>(input: &'a [u8]) -> IResult<&'a [u8], Event> {
        let (i, header) = Header::parse_v4_header(input)?;

        let c = LogContext::default();
        LogEventDecoder::parse_bytes(i, &header, Rc::new(RefCell::new(c)))
    }

}


#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
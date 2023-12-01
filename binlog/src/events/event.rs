use serde::Serialize;
use crate::events::event_header::Header;
use crate::mysql::{ColTypes, ColValues};
use crate::events::{query, rows};

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum Event {
    // ref: https://dev.mysql.com/doc/internals/en/ignored-events.html#unknown-event
    Unknown {
        header: Header,
        checksum: u32,
    },
    // doc: https://dev.mysql.com/doc/internals/en/query-event.html
    // source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/statement_events.h#L44-L426
    // layout: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/statement_events.h#L627-L643
    Query {
        header: Header,
        slave_proxy_id: u32, // thread_id
        execution_time: u32,
        schema_length: u8, // length of current select schema name
        error_code: u16,
        status_vars_length: u16,
        status_vars: Vec<query::QueryStatusVar>,
        schema: String,
        query: String,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/stop-event.html
    Stop {
        header: Header,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/rotate-event.html
    Rotate {
        header: Header,
        position: u64,
        next_binlog: String,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/intvar-event.html
    IntVar {
        header: Header,
        e_type: IntVarEventType,
        value: u64,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/load-event.html
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
    // ref: https://dev.mysql.com/doc/internals/en/ignored-events.html#slave-event
    Slave {
        header: Header,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/create-file-event.html
    CreateFile {
        header: Header,
        file_id: u32,
        block_data: String,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/append-block-event.html
    AppendBlock {
        header: Header,
        file_id: u32,
        block_data: String,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/exec-load-event.html
    ExecLoad {
        header: Header,
        file_id: u16,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/delete-file-event.html
    DeleteFile {
        header: Header,
        file_id: u16,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/new-load-event.html
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
    // ref: https://dev.mysql.com/doc/internals/en/rand-event.html
    Rand {
        header: Header,
        seed1: u64,
        seed2: u64,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/user-var-event.html
    // source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/statement_events.h#L712-L779
    // NOTE ref is broken !!!
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
    // source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L295-L344
    // event_data layout: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L387-L416
    FormatDesc {
        header: Header,
        binlog_version: u16,
        mysql_server_version: String,
        create_timestamp: u32,
        event_header_length: u8,
        supported_types: Vec<u8>,
        checksum_alg: u8,
        checksum: u32,
    },
    XID {
        header: Header,
        xid: u64,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/begin-load-query-event.html
    BeginLoadQuery {
        header: Header,
        file_id: u32,
        block_data: String,
        checksum: u32,
    },
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
    TableMap {
        header: Header,
        // table_id take 6 bytes in buffer
        table_id: u64,
        flags: u16,
        schema_length: u8,
        schema: String,
        // [00] term sign in layout
        table_name_length: u8,
        table_name: String,
        // [00] term sign in layout
        // len encoded integer
        column_count: u64,
        columns_type: Vec<ColTypes>,
        null_bits: Vec<u8>,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/incident-event.html
    Incident {
        header: Header,
        d_type: IncidentEventType,
        message_length: u8,
        message: String,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/heartbeat-event.html
    Heartbeat {
        header: Header,
        checksum: u32,
    },
    // ref: https://dev.mysql.com/doc/internals/en/rows-query-event.html
    RowQuery {
        header: Header,
        length: u8,
        query_text: String,
        checksum: u32,
    },
    // https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L1048-L1056
    Gtid {
        header: Header,
        rbr_only: bool,
        source_id: String,
        transaction_id: String,
        ts_type: u8,
        last_committed: i64,
        sequence_number: i64,
        checksum: u32,
    },
    AnonymousGtid {
        header: Header,
        rbr_only: bool,
        source_id: String,
        transaction_id: String,
        ts_type: u8,
        last_committed: i64,
        sequence_number: i64,
        checksum: u32,
    },
    // source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L1073-L1103
    PreviousGtids {
        header: Header,
        // TODO do more specify parse
        gtid_sets: Vec<u8>,
        buf_size: u32,
        checksum: u32,
    },
    // source https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/rows_event.h#L488-L613
    WriteRowsV2 {
        header: Header,
        // table_id take 6 bytes in buffer
        table_id: u64,
        flags: rows::Flags,
        extra_data_len: u16,
        extra_data: Vec<rows::ExtraData>,
        column_count: u64,
        inserted_image_bits: Vec<u8>,
        rows: Vec<Vec<ColValues>>,
        checksum: u32,
    },
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
}


#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum IntVarEventType {
    InvalidIntEvent,
    LastInsertIdEvent,
    InsertIdEvent,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct EmptyFlags {
    field_term_empty: bool,
    enclosed_empty: bool,
    line_term_empty: bool,
    line_start_empty: bool,
    escape_empty: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct OptFlags {
    dump_file: bool,
    opt_enclosed: bool,
    replace: bool,
    ignore: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum DupHandlingFlags {
    Error,
    Ignore,
    Replace,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum IncidentEventType {
    None,
    LostEvents,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub enum UserVarType {
    STRING = 0,
    REAL = 1,
    INT = 2,
    ROW = 3,
    DECIMAL = 4,
    VALUE_TYPE_COUNT = 5,
    Unknown,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
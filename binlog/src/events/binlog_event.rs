use crate::events::event_header::Header;
use crate::events::{
    query, DupHandlingFlags, EmptyFlags, IncidentEventType, OptFlags, UserVarType,
};

use crate::events::declare::log_event::LogEvent;
use crate::events::declare::rows_log_event::RowsLogEvent;
use crate::events::protocol::delete_rows_v12_event::DeleteRowsEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::alias::mysql::events::previous_gtids_event::PreviousGtidsLogEvent;
use crate::events::protocol::query_event::QueryEvent;
use crate::events::protocol::rotate_event::RotateEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
use crate::events::protocol::write_rows_v12_event::WriteRowsEvent;
use serde::Serialize;
use crate::alias::mysql::events::gtid_log_event::GtidLogEvent;
use crate::events::protocol::int_var_event::IntVarEvent;
use crate::events::protocol::slave_event::SlaveEvent;
use crate::events::protocol::stop_event::StopEvent;
use crate::events::protocol::unknown_event::UnknownEvent;
use crate::events::protocol::user_var_event::UserVarEvent;
use crate::events::protocol::v4::start_v3_event::StartV3Event;
use crate::events::protocol::xid_event::XidLogEvent;

///
/// Enumeration type for the different types of log events.
///
/// @see  https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html
///
/// ```text
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
/// ```
#[derive(Debug, Serialize, Clone)]
pub enum BinlogEvent {
    /// 0, ref: https://dev.mysql.com/doc/internals/en/ignored-events.html#unknown-event
    Unknown(UnknownEvent),
    /// 事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
    StartV3(StartV3Event),

    Query(QueryEvent),
    /// ref: https://dev.mysql.com/doc/internals/en/stop-event.html
    Stop(StopEvent),
    /// ref: https://dev.mysql.com/doc/internals/en/rotate-event.html
    Rotate(RotateEvent),
    /// 5, ref: https://dev.mysql.com/doc/internals/en/intvar-event.html
    IntVar(IntVarEvent),
    /// 6, ref: https://dev.mysql.com/doc/internals/en/load-event.html
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
    Slave(SlaveEvent),
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
    UserVar(UserVarEvent),
    /// 15
    FormatDescription(FormatDescriptionEvent),
    /// 16
    XID(XidLogEvent),
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
    TableMap(TableMapEvent),

    ///These event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
    /// 20
    PreGaWriteRowsEvent,
    /// 21
    PreGaUpdateRowsEvent,
    /// 22
    PreGaDeleteRowsEvent,

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
    Heartbeat { header: Header, checksum: u32 },

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

    /// These event numbers are used from 5.1.16 and forward The V1 event numbers are used from 5.1.16 until mysql-5.6.
    /// 23 WRITE_ROWS_V1, 24 UPDATE_ROWS_V1, 25 DELETE_ROWS_V1
    ///
    /// Version 2 of the Row events
    /// 30
    /// source https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/rows_event.h#L488-L613
    WriteRows(WriteRowsEvent),
    /// 31
    UpdateRows(UpdateRowsEvent),
    /// 32
    DeleteRows(DeleteRowsEvent),

    /// 33
    /// equals AnonymousGtidLog
    GtidLog(GtidLogEvent),
    /// 34
    /// equals GtidLog
    AnonymousGtidLog(GtidLogEvent),
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
    ENUM_END_EVENT,
}

impl BinlogEvent {
    /// Get the type name of the event for debugging and logging
    pub fn get_type_name(&self) -> &'static str {
        match self {
            BinlogEvent::Unknown(_) => "UnknownEvent",
            BinlogEvent::StartV3(_) => "StartV3Event",
            BinlogEvent::Query(_) => "QueryEvent",
            BinlogEvent::Stop(_) => "StopEvent",
            BinlogEvent::Rotate(_) => "RotateEvent",
            BinlogEvent::IntVar(_) => "IntVarEvent",
            BinlogEvent::Load { .. } => "LoadEvent",
            BinlogEvent::Slave(_) => "SlaveEvent",
            BinlogEvent::CreateFile { .. } => "CreateFileEvent",
            BinlogEvent::AppendBlock { .. } => "AppendBlockEvent",
            BinlogEvent::ExecLoad { .. } => "ExecLoadEvent",
            BinlogEvent::DeleteFile { .. } => "DeleteFileEvent",
            BinlogEvent::NewLoad { .. } => "NewLoadEvent",
            BinlogEvent::Rand { .. } => "RandEvent",
            BinlogEvent::UserVar(_) => "UserVarEvent",
            BinlogEvent::FormatDescription(_) => "FormatDescriptionEvent",
            BinlogEvent::XID(_) => "XIDEvent",
            BinlogEvent::BeginLoadQuery { .. } => "BeginLoadQueryEvent",
            BinlogEvent::ExecuteLoadQueryEvent { .. } => "ExecuteLoadQueryEvent",
            BinlogEvent::TableMap(_) => "TableMapEvent",
            BinlogEvent::PreGaWriteRowsEvent => "PreGaWriteRowsEvent",
            BinlogEvent::PreGaUpdateRowsEvent => "PreGaUpdateRowsEvent",
            BinlogEvent::PreGaDeleteRowsEvent => "PreGaDeleteRowsEvent",
            BinlogEvent::Incident { .. } => "IncidentEvent",
            BinlogEvent::Heartbeat { .. } => "HeartbeatEvent",
            BinlogEvent::IgnorableLogEvent => "IgnorableLogEvent",
            BinlogEvent::RowQuery { .. } => "RowQueryEvent",
            BinlogEvent::WriteRows(_) => "WriteRowsEvent",
            BinlogEvent::UpdateRows(_) => "UpdateRowsEvent",
            BinlogEvent::DeleteRows(_) => "DeleteRowsEvent",
            BinlogEvent::GtidLog(_) => "GtidLogEvent",
            BinlogEvent::AnonymousGtidLog(_) => "AnonymousGtidLogEvent",
            BinlogEvent::PreviousGtidsLog(_) => "PreviousGtidsLogEvent",
            BinlogEvent::TRANSACTION_CONTEXT => "TransactionContextEvent",
            BinlogEvent::VIEW_CHANGE => "ViewChangeEvent",
            BinlogEvent::XA_PREPARE_LOG => "XaPrepareLogEvent",
            BinlogEvent::PARTIAL_UPDATE_ROWS => "PartialUpdateRowsEvent",
            BinlogEvent::TRANSACTION_PAYLOAD => "TransactionPayloadEvent",
            BinlogEvent::HEARTBEAT_LOG_V2 => "HeartbeatLogV2Event",
            BinlogEvent::MYSQL_ENUM_END => "MysqlEnumEndEvent",
            BinlogEvent::ENUM_END_EVENT => "EnumEndEvent",
        }
    }

    /// Get the event type code
    pub fn get_event_type_code(&self) -> u8 {
        match self {
            BinlogEvent::Unknown(_) => 0,
            BinlogEvent::StartV3(_) => 1,
            BinlogEvent::Query(_) => 2,
            BinlogEvent::Stop(_) => 3,
            BinlogEvent::Rotate(_) => 4,
            BinlogEvent::IntVar(_) => 5,
            BinlogEvent::Load { .. } => 6,
            BinlogEvent::Slave(_) => 7,
            BinlogEvent::CreateFile { .. } => 8,
            BinlogEvent::AppendBlock { .. } => 9,
            BinlogEvent::ExecLoad { .. } => 10,
            BinlogEvent::DeleteFile { .. } => 11,
            BinlogEvent::NewLoad { .. } => 12,
            BinlogEvent::Rand { .. } => 13,
            BinlogEvent::UserVar(_) => 14,
            BinlogEvent::FormatDescription(_) => 15,
            BinlogEvent::XID(_) => 16,
            BinlogEvent::BeginLoadQuery { .. } => 17,
            BinlogEvent::ExecuteLoadQueryEvent { .. } => 18,
            BinlogEvent::TableMap(_) => 19,
            BinlogEvent::PreGaWriteRowsEvent => 20,
            BinlogEvent::PreGaUpdateRowsEvent => 21,
            BinlogEvent::PreGaDeleteRowsEvent => 22,
            BinlogEvent::WriteRows(_) => 30, // Also handles WRITE_ROWS_EVENT_V1 (23)
            BinlogEvent::UpdateRows(_) => 31, // Also handles UPDATE_ROWS_EVENT_V1 (24)
            BinlogEvent::DeleteRows(_) => 32, // Also handles DELETE_ROWS_EVENT_V1 (25)
            BinlogEvent::Incident { .. } => 26,
            BinlogEvent::Heartbeat { .. } => 27,
            BinlogEvent::IgnorableLogEvent => 28,
            BinlogEvent::RowQuery { .. } => 29,
            BinlogEvent::GtidLog(_) => 33,
            BinlogEvent::AnonymousGtidLog(_) => 34,
            BinlogEvent::PreviousGtidsLog(_) => 35,
            BinlogEvent::TRANSACTION_CONTEXT => 36,
            BinlogEvent::VIEW_CHANGE => 37,
            BinlogEvent::XA_PREPARE_LOG => 38,
            BinlogEvent::PARTIAL_UPDATE_ROWS => 39,
            BinlogEvent::TRANSACTION_PAYLOAD => 40,
            BinlogEvent::HEARTBEAT_LOG_V2 => 41,
            BinlogEvent::MYSQL_ENUM_END => 42,
            BinlogEvent::ENUM_END_EVENT => 255,
        }
    }

    /// Check if this is a row-level event
    pub fn is_row_event(&self) -> bool {
        matches!(self, 
            BinlogEvent::WriteRows(_) | 
            BinlogEvent::UpdateRows(_) | 
            BinlogEvent::DeleteRows(_) |
            BinlogEvent::PreGaWriteRowsEvent |
            BinlogEvent::PreGaUpdateRowsEvent |
            BinlogEvent::PreGaDeleteRowsEvent
        )
    }

    /// Check if this is a GTID-related event
    pub fn is_gtid_event(&self) -> bool {
        matches!(self,
            BinlogEvent::GtidLog(_) |
            BinlogEvent::AnonymousGtidLog(_) |
            BinlogEvent::PreviousGtidsLog(_)
        )
    }

    /// Check if this event affects table structure
    pub fn is_ddl_event(&self) -> bool {
        match self {
            BinlogEvent::Query(query_event) => {
                // This would need to be implemented based on the query content
                // For now, we assume all query events could be DDL
                true
            },
            _ => false,
        }
    }

    /// Get the table ID if this event is table-specific
    pub fn get_table_id(&self) -> Option<u64> {
        match self {
            BinlogEvent::TableMap(event) => Some(event.table_id),
            BinlogEvent::WriteRows(event) => Some(event.table_id),
            BinlogEvent::UpdateRows(event) => Some(event.table_id),
            BinlogEvent::DeleteRows(event) => Some(event.table_id),
            _ => None,
        }
    }

    /// Get debug information for the event
    pub fn get_debug_info(&self) -> String {
        format!("{}(len: {})", self.get_type_name(), self.len())
    }

    /// Get the event length in bytes
    pub fn len(&self) -> u32 {
        match self {
            BinlogEvent::Unknown(e) => e.len() as u32,
            BinlogEvent::StartV3(e) => e.len() as u32,
            BinlogEvent::Query(e) => e.len() as u32,
            BinlogEvent::Stop(e) => e.len() as u32,
            BinlogEvent::Rotate(e) => e.len() as u32,
            BinlogEvent::IntVar(e) => e.len() as u32,
            BinlogEvent::Slave(e) => e.len() as u32,
            BinlogEvent::UserVar(e) => e.len() as u32,
            BinlogEvent::FormatDescription(e) => e.len() as u32,
            BinlogEvent::XID(e) => e.len() as u32,
            BinlogEvent::TableMap(e) => e.len() as u32,
            BinlogEvent::WriteRows(e) => e.len() as u32,
            BinlogEvent::UpdateRows(e) => e.len() as u32,
            BinlogEvent::DeleteRows(e) => e.len() as u32,
            BinlogEvent::GtidLog(e) => e.len() as u32,
            BinlogEvent::AnonymousGtidLog(e) => e.len() as u32,
            BinlogEvent::PreviousGtidsLog(e) => e.len() as u32,

            BinlogEvent::Load { header, .. } |
            BinlogEvent::CreateFile { header, ..  }  |
            BinlogEvent::AppendBlock { header, ..  }  |
            BinlogEvent::ExecLoad { header, ..  }  |
            BinlogEvent::DeleteFile { header, ..  }  |
            BinlogEvent::NewLoad { header, ..  }  |
            BinlogEvent::Rand { header, ..  }  |
            BinlogEvent::BeginLoadQuery { header, ..  }  |
            BinlogEvent::ExecuteLoadQueryEvent { header, ..  }  |
            BinlogEvent::Incident { header, ..  }  |
            BinlogEvent::Heartbeat { header, ..  }  |
            BinlogEvent::RowQuery { header, ..  } => header.get_event_length(),

            BinlogEvent::IgnorableLogEvent |
            BinlogEvent::PreGaWriteRowsEvent |
            BinlogEvent::PreGaUpdateRowsEvent |
            BinlogEvent::PreGaDeleteRowsEvent |
            BinlogEvent::TRANSACTION_CONTEXT |
            BinlogEvent::VIEW_CHANGE |
            BinlogEvent::XA_PREPARE_LOG |
            BinlogEvent::PARTIAL_UPDATE_ROWS |
            BinlogEvent::TRANSACTION_PAYLOAD |
            BinlogEvent::HEARTBEAT_LOG_V2 |
            BinlogEvent::MYSQL_ENUM_END |
            BinlogEvent::ENUM_END_EVENT => 0,
        }
    }

    /// Check if the event is empty (has no data)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage estimate for this event
    pub fn memory_usage(&self) -> usize {
        // Use event length as approximation for memory usage
        std::mem::size_of::<BinlogEvent>() + self.len() as usize
    }

    /// Validate the event structure and data integrity
    pub fn validate(&self) -> Result<(), common::err::decode_error::ReError> {
        use common::err::decode_error::{ReError, ErrorContext};

        let context = ErrorContext::new()
            .with_event_type(self.get_event_type_code())
            .with_operation("validate_event".to_string());

        // Basic length validation
        if self.len() == 0 && !matches!(self, 
            BinlogEvent::IgnorableLogEvent |
            BinlogEvent::PreGaWriteRowsEvent |
            BinlogEvent::PreGaUpdateRowsEvent |
            BinlogEvent::PreGaDeleteRowsEvent |
            BinlogEvent::TRANSACTION_CONTEXT |
            BinlogEvent::VIEW_CHANGE |
            BinlogEvent::XA_PREPARE_LOG |
            BinlogEvent::PARTIAL_UPDATE_ROWS |
            BinlogEvent::TRANSACTION_PAYLOAD |
            BinlogEvent::HEARTBEAT_LOG_V2 |
            BinlogEvent::MYSQL_ENUM_END |
            BinlogEvent::ENUM_END_EVENT
        ) {
            return Err(ReError::invalid_data_format(
                "Event has zero length".to_string(),
                context
            ));
        }

        // Basic validation - more specific validation would require access to private fields
        Ok(())
    }

    /// Get the timestamp of the event
    pub fn get_timestamp(&self) -> Option<u32> {
        match self {
            BinlogEvent::Load { header, .. } |
            BinlogEvent::CreateFile { header, .. } |
            BinlogEvent::AppendBlock { header, .. } |
            BinlogEvent::ExecLoad { header, .. } |
            BinlogEvent::DeleteFile { header, .. } |
            BinlogEvent::NewLoad { header, .. } |
            BinlogEvent::Rand { header, .. } |
            BinlogEvent::BeginLoadQuery { header, .. } |
            BinlogEvent::ExecuteLoadQueryEvent { header, .. } |
            BinlogEvent::Incident { header, .. } |
            BinlogEvent::Heartbeat { header, .. } |
            BinlogEvent::RowQuery { header, .. } => Some(header.when),
            
            _ => None, // Header fields are private in most event types
        }
    }

    /// Get the server ID of the event
    pub fn get_server_id(&self) -> Option<u32> {
        match self {
            BinlogEvent::Load { header, .. } |
            BinlogEvent::CreateFile { header, .. } |
            BinlogEvent::AppendBlock { header, .. } |
            BinlogEvent::ExecLoad { header, .. } |
            BinlogEvent::DeleteFile { header, .. } |
            BinlogEvent::NewLoad { header, .. } |
            BinlogEvent::Rand { header, .. } |
            BinlogEvent::BeginLoadQuery { header, .. } |
            BinlogEvent::ExecuteLoadQueryEvent { header, .. } |
            BinlogEvent::Incident { header, .. } |
            BinlogEvent::Heartbeat { header, .. } |
            BinlogEvent::RowQuery { header, .. } => Some(header.server_id),
            
            _ => None, // Header fields are private in most event types
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}

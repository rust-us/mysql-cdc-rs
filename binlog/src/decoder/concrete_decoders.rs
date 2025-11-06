use std::collections::HashMap;
use std::io::Cursor;
use common::err::decode_error::{ReError, ErrorContext};
use crate::b_type::LogEventType;
use crate::events::binlog_event::BinlogEvent;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::declare::log_event::LogEvent;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::decoder::event_decoder_registry::{EventDecoder, DecoderPriority};
use crate::events::protocol::table_map_event::TableMapEvent;

// Import all the event types we need
use crate::events::protocol::unknown_event::UnknownEvent;
use crate::events::protocol::v4::start_v3_event::StartV3Event;
use crate::events::protocol::query_event::QueryEvent;
use crate::events::protocol::stop_event::StopEvent;
use crate::events::protocol::rotate_event::RotateEvent;
use crate::events::protocol::int_var_event::IntVarEvent;
use crate::events::protocol::slave_event::SlaveEvent;
use crate::events::protocol::user_var_event::UserVarEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::xid_event::XidLogEvent;
use crate::events::protocol::ignorable_log_event::IgnorableLogEvent;
use crate::events::protocol::write_rows_v12_event::WriteRowsEvent;
use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
use crate::events::protocol::delete_rows_v12_event::DeleteRowsEvent;
use crate::alias::mysql::events::gtid_log_event::GtidLogEvent;
use crate::alias::mysql::events::previous_gtids_event::PreviousGtidsLogEvent;
use crate::events::protocol::anonymous_gtid_log_event::AnonymousGtidLogEvent;

// Import parser functions
use crate::decoder::event_decoder_impl::{
    parse_load, parse_create_file, parse_append_block, parse_exec_load, 
    parse_delete_file, parse_new_load, parse_rand, parse_begin_load_query,
    parse_execute_load_query, parse_incident, parse_heartbeat, parse_row_query
};

// Additional imports for I/O operations
use std::io::Read;

/// Decoder for Unknown events
#[derive(Debug)]
pub struct UnknownEventDecoder;

impl EventDecoder for UnknownEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::UNKNOWN_EVENT.as_val() as u8 }
    fn name(&self) -> &'static str { "UnknownEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = UnknownEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::Unknown(event))
    }
}

/// Decoder for StartV3 events
#[derive(Debug)]
pub struct StartV3EventDecoder;

impl EventDecoder for StartV3EventDecoder {
    fn event_type(&self) -> u8 { LogEventType::START_EVENT_V3.as_val() as u8 }
    fn name(&self) -> &'static str { "StartV3EventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = StartV3Event::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::StartV3(event))
    }
}

/// Decoder for Query events
#[derive(Debug)]
pub struct QueryEventDecoder;

impl EventDecoder for QueryEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::QUERY_EVENT.as_u8() }
    fn name(&self) -> &'static str { "QueryEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = QueryEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::Query(event))
    }
}

/// Decoder for Stop events
#[derive(Debug)]
pub struct StopEventDecoder;

impl EventDecoder for StopEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::STOP_EVENT.as_u8() }
    fn name(&self) -> &'static str { "StopEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = StopEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::Stop(event))
    }
}

/// Decoder for Rotate events
#[derive(Debug)]
pub struct RotateEventDecoder;

impl EventDecoder for RotateEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::ROTATE_EVENT.as_u8() }
    fn name(&self) -> &'static str { "RotateEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = RotateEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::Rotate(event))
    }
}

/// Decoder for IntVar events
#[derive(Debug)]
pub struct IntVarEventDecoder;

impl EventDecoder for IntVarEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::INTVAR_EVENT.as_u8() }
    fn name(&self) -> &'static str { "IntVarEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = IntVarEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::IntVar(event))
    }
}

/// Decoder for TableMap events
#[derive(Debug)]
pub struct TableMapEventDecoder;

impl EventDecoder for TableMapEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::TABLE_MAP_EVENT.as_u8() }
    fn name(&self) -> &'static str { "TableMapEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Critical }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = TableMapEvent::parse(cursor, header, context, table_map, table_cache_manager)?;
        Ok(BinlogEvent::TableMap(event))
    }
}

/// Decoder for WriteRows events (handles both V1 and V2)
#[derive(Debug)]
pub struct WriteRowsEventDecoder;

impl EventDecoder for WriteRowsEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::WRITE_ROWS_EVENT.as_u8() }
    fn name(&self) -> &'static str { "WriteRowsEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn can_decode(&self, event_type: u8, data: &[u8]) -> bool {
        (event_type == LogEventType::WRITE_ROWS_EVENT.as_u8() || 
         event_type == LogEventType::WRITE_ROWS_EVENT_V1.as_u8()) && !data.is_empty()
    }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = WriteRowsEvent::parse(cursor, header, context, table_map, None)?;
        Ok(BinlogEvent::WriteRows(event))
    }
}

/// Decoder for UpdateRows events (handles both V1 and V2)
#[derive(Debug)]
pub struct UpdateRowsEventDecoder;

impl EventDecoder for UpdateRowsEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::UPDATE_ROWS_EVENT.as_u8() }
    fn name(&self) -> &'static str { "UpdateRowsEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn can_decode(&self, event_type: u8, data: &[u8]) -> bool {
        (event_type == LogEventType::UPDATE_ROWS_EVENT.as_u8() || 
         event_type == LogEventType::UPDATE_ROWS_EVENT_V1.as_u8()) && !data.is_empty()
    }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = UpdateRowsEvent::parse(cursor, header, context, table_map, None)?;
        Ok(BinlogEvent::UpdateRows(event))
    }
}

/// Decoder for DeleteRows events (handles both V1 and V2)
#[derive(Debug)]
pub struct DeleteRowsEventDecoder;

impl EventDecoder for DeleteRowsEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::DELETE_ROWS_EVENT.as_u8() }
    fn name(&self) -> &'static str { "DeleteRowsEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn can_decode(&self, event_type: u8, data: &[u8]) -> bool {
        (event_type == LogEventType::DELETE_ROWS_EVENT.as_u8() || 
         event_type == LogEventType::DELETE_ROWS_EVENT_V1.as_u8()) && !data.is_empty()
    }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = DeleteRowsEvent::parse(cursor, header, context, table_map, None)?;
        Ok(BinlogEvent::DeleteRows(event))
    }
}

/// Decoder for GTID events
#[derive(Debug)]
pub struct GtidLogEventDecoder;

impl EventDecoder for GtidLogEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::GTID_LOG_EVENT.as_u8() }
    fn name(&self) -> &'static str { "GtidLogEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Critical }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = GtidLogEvent::parse(cursor, header, context, table_map, None)?;
        Ok(BinlogEvent::GtidLog(event))
    }
}

/// Decoder for Anonymous GTID events
#[derive(Debug)]
pub struct AnonymousGtidLogEventDecoder;

impl EventDecoder for AnonymousGtidLogEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::ANONYMOUS_GTID_LOG_EVENT.as_u8() }
    fn name(&self) -> &'static str { "AnonymousGtidLogEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Critical }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let anonymous_event = AnonymousGtidLogEvent::parse(cursor, header, context, table_map, None)?;
        Ok(BinlogEvent::AnonymousGtidLog(anonymous_event.gtid_event))
    }
}

/// Decoder for Previous GTIDs events
#[derive(Debug)]
pub struct PreviousGtidsLogEventDecoder;

impl EventDecoder for PreviousGtidsLogEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::PREVIOUS_GTIDS_LOG_EVENT.as_u8() }
    fn name(&self) -> &'static str { "PreviousGtidsLogEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = PreviousGtidsLogEvent::parse(cursor, header, context, table_map, None)?;
        Ok(BinlogEvent::PreviousGtidsLog(event))
    }
}

/// Decoder for Format Description events
#[derive(Debug)]
pub struct FormatDescriptionEventDecoder;

impl EventDecoder for FormatDescriptionEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::FORMAT_DESCRIPTION_EVENT.as_u8() }
    fn name(&self) -> &'static str { "FormatDescriptionEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Critical }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = FormatDescriptionEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::FormatDescription(event))
    }
}

/// Decoder for XID events
#[derive(Debug)]
pub struct XidLogEventDecoder;

impl EventDecoder for XidLogEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::XID_EVENT.as_u8() }
    fn name(&self) -> &'static str { "XidLogEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = XidLogEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::XID(event))
    }
}

/// Decoder for Rand events
#[derive(Debug)]
pub struct RandEventDecoder;

impl EventDecoder for RandEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::RAND_EVENT.as_u8() }
    fn name(&self) -> &'static str { "RandEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let remaining_bytes = cursor.get_ref().len() - cursor.position() as usize;
        let mut buffer = vec![0u8; remaining_bytes];
        cursor.read_exact(&mut buffer).map_err(|e| ReError::IoError(e))?;
        
        let result = parse_rand(&buffer, header);
        match result {
            Ok((_, event)) => Ok(event),
            Err(e) => Err(ReError::EventParseError {
                message: format!("Failed to parse RAND event: {:?}", e),
                context: ErrorContext::new().with_operation("parse_rand".to_string()),
                source: None,
            })
        }
    }
}

/// Decoder for UserVar events
#[derive(Debug)]
pub struct UserVarEventDecoder;

impl EventDecoder for UserVarEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::USER_VAR_EVENT.as_u8() }
    fn name(&self) -> &'static str { "UserVarEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let event = UserVarEvent::parse(cursor, header, context, None, None)?;
        Ok(BinlogEvent::UserVar(event))
    }
}

/// Decoder for BeginLoadQuery events
#[derive(Debug)]
pub struct BeginLoadQueryEventDecoder;

impl EventDecoder for BeginLoadQueryEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::BEGIN_LOAD_QUERY_EVENT.as_u8() }
    fn name(&self) -> &'static str { "BeginLoadQueryEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let remaining_bytes = cursor.get_ref().len() - cursor.position() as usize;
        let mut buffer = vec![0u8; remaining_bytes];
        cursor.read_exact(&mut buffer).map_err(|e| ReError::IoError(e))?;
        
        let result = parse_begin_load_query(&buffer, header);
        match result {
            Ok((_, event)) => Ok(event),
            Err(e) => Err(ReError::EventParseError {
                message: format!("Failed to parse BEGIN_LOAD_QUERY event: {:?}", e),
                context: ErrorContext::new().with_operation("parse_begin_load_query".to_string()),
                source: None,
            })
        }
    }
}

/// Decoder for ExecuteLoadQuery events
#[derive(Debug)]
pub struct ExecuteLoadQueryEventDecoder;

impl EventDecoder for ExecuteLoadQueryEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::EXECUTE_LOAD_QUERY_EVENT.as_u8() }
    fn name(&self) -> &'static str { "ExecuteLoadQueryEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let remaining_bytes = cursor.get_ref().len() - cursor.position() as usize;
        let mut buffer = vec![0u8; remaining_bytes];
        cursor.read_exact(&mut buffer).map_err(|e| ReError::IoError(e))?;
        
        let result = parse_execute_load_query(&buffer, header);
        match result {
            Ok((_, event)) => Ok(event),
            Err(e) => Err(ReError::EventParseError {
                message: format!("Failed to parse EXECUTE_LOAD_QUERY event: {:?}", e),
                context: ErrorContext::new().with_operation("parse_execute_load_query".to_string()),
                source: None,
            })
        }
    }
}

/// Decoder for RowsQuery events
#[derive(Debug)]
pub struct RowsQueryLogEventDecoder;

impl EventDecoder for RowsQueryLogEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::ROWS_QUERY_LOG_EVENT.as_u8() }
    fn name(&self) -> &'static str { "RowsQueryLogEventDecoder" }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let remaining_bytes = cursor.get_ref().len() - cursor.position() as usize;
        let mut buffer = vec![0u8; remaining_bytes];
        cursor.read_exact(&mut buffer).map_err(|e| ReError::IoError(e))?;
        
        let result = parse_row_query(&buffer, header);
        match result {
            Ok((_, event)) => Ok(event),
            Err(e) => Err(ReError::EventParseError {
                message: format!("Failed to parse ROWS_QUERY event: {:?}", e),
                context: ErrorContext::new().with_operation("parse_rows_query".to_string()),
                source: None,
            })
        }
    }
}

/// Function to register all standard decoders
pub fn register_standard_decoders(registry: &crate::decoder::event_decoder_registry::EventDecoderRegistry) -> Result<(), ReError> {
    // Register all standard event decoders
    registry.register_decoder(Box::new(UnknownEventDecoder))?;
    registry.register_decoder(Box::new(StartV3EventDecoder))?;
    registry.register_decoder(Box::new(QueryEventDecoder))?;
    registry.register_decoder(Box::new(StopEventDecoder))?;
    registry.register_decoder(Box::new(RotateEventDecoder))?;
    registry.register_decoder(Box::new(IntVarEventDecoder))?;
    registry.register_decoder(Box::new(TableMapEventDecoder))?;
    registry.register_decoder(Box::new(WriteRowsEventDecoder))?;
    registry.register_decoder(Box::new(UpdateRowsEventDecoder))?;
    registry.register_decoder(Box::new(DeleteRowsEventDecoder))?;
    registry.register_decoder(Box::new(GtidLogEventDecoder))?;
    registry.register_decoder(Box::new(AnonymousGtidLogEventDecoder))?;
    registry.register_decoder(Box::new(PreviousGtidsLogEventDecoder))?;
    registry.register_decoder(Box::new(FormatDescriptionEventDecoder))?;
    registry.register_decoder(Box::new(XidLogEventDecoder))?;
    
    // Register additional event decoders
    registry.register_decoder(Box::new(RandEventDecoder))?;
    registry.register_decoder(Box::new(UserVarEventDecoder))?;
    registry.register_decoder(Box::new(BeginLoadQueryEventDecoder))?;
    registry.register_decoder(Box::new(ExecuteLoadQueryEventDecoder))?;
    registry.register_decoder(Box::new(RowsQueryLogEventDecoder))?;

    // Register MySQL 8.0+ specific decoders
    crate::decoder::mysql8_decoders::register_mysql8_decoders(registry)?;

    Ok(())
}
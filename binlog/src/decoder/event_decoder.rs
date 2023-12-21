use std::sync::{Arc, RwLock};
use nom::IResult;
use common::err::DecodeError::ReError;
use crate::b_type::LogEventType;
use crate::decoder::event_decoder_impl::*;

use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::protocol::anonymous_gtid_log_event::AnonymousGtidLogEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::gtid_log_event::GtidLogEvent;
use crate::events::protocol::previous_gtids_event::PreviousGtidsLogEvent;
use crate::events::protocol::query_event::QueryEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

pub trait EventDecoder {


    ///
    ///
    /// # Arguments
    ///
    /// * `raw`:  解析的字节码
    /// * `context`:
    ///
    /// returns: Result<(Event, Vec<u8, Global>), ReError>
    ///             Event 解析事件
    ///             &[u8]  剩余的未解析字节码
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn decode_with_raw(&mut self, raw: &EventRaw, context: Arc<RwLock<LogContext>>) -> Result<(Event, Vec<u8>), ReError>;

    ///
    ///
    /// # Arguments
    ///
    /// * `slice`:  解析的字节码
    /// * `header`:
    /// * `context`:
    ///
    /// returns: Result<(Event, Vec<u8, Global>), ReError>
    ///             Event 解析事件
    ///             &[u8]  剩余的未解析字节码
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn decode_with_slice(&mut self, slice: &[u8], header: &Header, context: Arc<RwLock<LogContext>>) -> Result<(Event, Vec<u8>), ReError>;
}

pub struct LogEventDecoder {
    //// Gets checksum algorithm type used in a binlog file/ bytes.
    need_le_checksum: bool,

    // /// Gets TableMapEvent cache required in row events.
    // table_map: HashMap<u64, TableMapEvent>,
}

impl EventDecoder for LogEventDecoder {
    fn decode_with_raw(&mut self, raw: &EventRaw, context: Arc<RwLock<LogContext>>) -> Result<(Event, Vec<u8>), ReError> {
        let header = raw.get_header_ref();
        let i = raw.get_payload();

        self.decode_with_slice(i, header.as_ref(), context)
    }

    fn decode_with_slice(&mut self, slice: &[u8], header: &Header, context: Arc<RwLock<LogContext>>) -> Result<(Event, Vec<u8>), ReError> {
         match LogEventDecoder::parse_bytes(slice, header, context) {
            Err(e) => return Err(ReError::Error(e.to_string())),
            Ok((i1, o)) => {
                Ok((o, i1.to_vec()))
            }
        }
    }
}

impl LogEventDecoder {
    pub fn new() -> Self {
        Self {
            need_le_checksum: true,
        }
    }

    /// 接口应该为私有
    pub fn parse_bytes<'a>(input: &'a [u8], header: &Header,
                           mut context: Arc<RwLock<LogContext>>) -> IResult<&'a [u8], Event> {
        let b_type = header.event_type;

        let type_ = LogEventType::from(b_type);
        match type_ {
            LogEventType::UNKNOWN_EVENT => parse_unknown(input, &header),
            // 1 START_EVENT_V3事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
            LogEventType::START_EVENT_V3 => {
                unreachable!();
            },
            LogEventType::QUERY_EVENT => {
                let (i, event) = QueryEvent::parse(input, &header, context.clone())?;
                /* updating position in context */
                context.write().unwrap().set_log_position_with_offset(header.get_log_pos());
                // header.putGtid

                Ok((i, Event::Query(event)))
            },
            LogEventType::STOP_EVENT => parse_stop(input, &header),
            LogEventType::ROTATE_EVENT => parse_rotate(input, &header),
            LogEventType::INTVAR_EVENT => parse_intvar(input, &header),
            LogEventType::LOAD_EVENT => parse_load(input, &header),
            LogEventType::SLAVE_EVENT => parse_slave(input, &header),
            LogEventType::CREATE_FILE_EVENT => parse_create_file(input, &header),
            LogEventType::APPEND_BLOCK_EVENT => parse_append_block(input, &header),  // 9
            LogEventType::EXEC_LOAD_EVENT => parse_exec_load(input, &header),     // 10
            LogEventType::DELETE_FILE_EVENT => parse_delete_file(input, &header),   // 11
            LogEventType::NEW_LOAD_EVENT => parse_new_load(input, &header),      // 12
            LogEventType::RAND_EVENT => parse_rand(input, &header),          // 13
            LogEventType::USER_VAR_EVENT => parse_user_var(input, &header),      // 14
            LogEventType::FORMAT_DESCRIPTION_EVENT => {   // 15
                let (i, event) = FormatDescriptionEvent::parse(input, &header)?;
                /* updating position in context */
                context.write().unwrap().set_log_position_with_offset(header.get_log_pos());
                context.write().unwrap().set_format_description(event.clone());

                Ok((i, Event::FormatDescription(event)))
            },
            LogEventType::XID_EVENT => parse_xid(input, &header),           // 16
            LogEventType::BEGIN_LOAD_QUERY_EVENT => parse_begin_load_query(input, &header),      // 17
            LogEventType::EXECUTE_LOAD_QUERY_EVENT => parse_execute_load_query(input, &header),    // 18
            LogEventType::TABLE_MAP_EVENT => {     // 19
                let (i, event) = TableMapEvent::parse(input, &header, context.clone())?;
                /* updating position in context */
                context.write().unwrap().set_log_position_with_offset(header.get_log_pos());
                context.write().unwrap().put_table(event.get_table_id(), event.clone());

                Ok((i, Event::TableMap(event)))
            },
            // 20, PreGaWriteRowsEvent， unreachable
            // 21, PreGaUpdateRowsEvent， unreachable
            // 22, PreGaDeleteRowsEvent， unreachable
            // 23, WriteRowsEventV1， unreachable
            // 24, UpdateRowsEventV1， unreachable
            // 25, DeleteRowsEventV1， unreachable
            // LogEventType::PRE_GA_WRITE_ROWS_EVENT..=LogEventType::DELETE_ROWS_EVENT_V1 => unreachable!(),

            LogEventType::INCIDENT_EVENT => parse_incident(input, &header),      // 26
            LogEventType::HEARTBEAT_LOG_EVENT => parse_heartbeat(input, &header),     // 27
            // 28 IgnorableLogEvent
            LogEventType::ROWS_QUERY_LOG_EVENT => parse_row_query(input, &header),     // 29
            LogEventType::WRITE_ROWS_EVENT => parse_write_rows_v2(input, &header), // 30
            LogEventType::UPDATE_ROWS_EVENT => parse_update_rows_v2(input, &header),// 31
            LogEventType::DELETE_ROWS_EVENT => parse_delete_rows_v2(input, &header),// 32
            LogEventType::GTID_LOG_EVENT => { // 33
                let (i, event) = GtidLogEvent::parse(input, &header)?;
                /* updating position in context */
                context.write().unwrap().set_log_position_with_offset(header.get_log_pos());
                // update latest gtid
                // setGtidLogEvent

                Ok((i, Event::GtidLog(event)))
            },
            LogEventType::ANONYMOUS_GTID_LOG_EVENT => { // 34
                let (i, event) = AnonymousGtidLogEvent::parse(input, &header)?;
                /* updating position in context */
                context.write().unwrap().set_log_position_with_offset(header.get_log_pos());
                // update latest gtid
                // setGtidLogEvent

                Ok((i, Event::AnonymousGtidLog(event)))
            },
            LogEventType::PREVIOUS_GTIDS_LOG_EVENT => {  // 35
                let (i, event) = PreviousGtidsLogEvent::parse(input, &header)?;
                /* updating position in context */
                context.write().unwrap().set_log_position_with_offset(header.get_log_pos());

                Ok((i, Event::PreviousGtidsLog(event)))
            },
            // TRANSACTION_CONTEXT_EVENT 36
            // VIEW_CHANGE_EVENT  37
            // XA_PREPARE_LOG_EVENT  38
            // PARTIAL_UPDATE_ROWS_EVENT
            // TRANSACTION_PAYLOAD_EVENT
            // @see https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html#a4a991abea842d4e50cbee0e490c28ceea1b1312ed0f5322b720ab2b957b0e9999
            // HEARTBEAT_LOG_EVENT_V2
            // ENUM_END_EVENT
            t @ _ => {
                log::error!("unexpected event type: {:x}", t.as_val());
                unreachable!();
            }
        }
    }
}

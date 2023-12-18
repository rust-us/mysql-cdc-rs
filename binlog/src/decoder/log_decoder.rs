use std::cell::RefCell;
use std::rc::Rc;
use nom::IResult;
use common::err::DecodeError::DecodeError;
use crate::b_type::LogEventType;
use crate::decoder::event_decoder::*;

use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::protocol::anonymous_gtid_log_event::AnonymousGtidLogEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::gtid_log_event::GtidLogEvent;
use crate::events::protocol::previous_gtids_event::PreviousGtidsLogEvent;
use crate::events::protocol::query_event::QueryEvent;

pub trait LogDecoder {

    ///
    ///
    /// # Arguments
    ///
    /// * `bytes`: 解析的字节码
    ///
    /// returns: Result<(&[u8], Vec<Event, Global>), Err<Error<&[u8]>>>
    ///           &[u8]  剩余的未解析字节码
    ///           Vec<Event>   解析事件
    fn decode<'a>(raw: &EventRaw, context_ref: Rc<RefCell<LogContext>>) -> Result<EventParser, DecodeError>;
}

pub struct LogEventDecoder {

}

pub struct EventParser {
    pub event: Event,

    pub remain_bytes: Vec<u8>,
}

impl LogDecoder for LogEventDecoder {
    fn decode<'a>(raw: &EventRaw, context_ref: Rc<RefCell<LogContext>>) -> Result<EventParser, DecodeError> {
        let header = raw.get_header();
        let i = raw.get_payload();

        match LogEventDecoder::parse_bytes(i, header, Rc::clone(&context_ref)) {
            Err(e) => return Err(DecodeError::Error(e.to_string())),
            Ok((i1, o)) => {
                Ok(EventParser::new(o, i1.to_vec()))
            }
        }
    }
}

impl LogEventDecoder {

    pub fn parse_bytes<'a>(input: &'a [u8], header_ref:Rc<&Header>,
                           mut context_ref: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Event> {
        let b_type = header_ref.event_type;

        let type_ = LogEventType::from(b_type);
        match type_ {
            LogEventType::UNKNOWN_EVENT => parse_unknown(input, header_ref),
            // 1 START_EVENT_V3事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
            LogEventType::START_EVENT_V3 => {
                unreachable!();
            },
            LogEventType::QUERY_EVENT => {
                let (i, event) = QueryEvent::parse(input, header_ref.clone(), context_ref.clone())?;
                /* updating position in context */
                context_ref.borrow_mut().clone().set_log_position_with_offset(header_ref.get_log_pos());
                // header.putGtid

                Ok((i,
                    Event::Query {
                        event,
                    },
                ))
            },
            LogEventType::STOP_EVENT => parse_stop(input, header_ref),
            LogEventType::ROTATE_EVENT => parse_rotate(input, header_ref),
            LogEventType::INTVAR_EVENT => parse_intvar(input, header_ref),
            LogEventType::LOAD_EVENT => parse_load(input, header_ref),
            LogEventType::SLAVE_EVENT => parse_slave(input, header_ref),
            LogEventType::CREATE_FILE_EVENT => parse_create_file(input, header_ref),
            LogEventType::APPEND_BLOCK_EVENT => parse_append_block(input, header_ref),  // 9
            LogEventType::EXEC_LOAD_EVENT => parse_exec_load(input, header_ref),     // 10
            LogEventType::DELETE_FILE_EVENT => parse_delete_file(input, header_ref),   // 11
            LogEventType::NEW_LOAD_EVENT => parse_new_load(input, header_ref),      // 12
            LogEventType::RAND_EVENT => parse_rand(input, header_ref),          // 13
            LogEventType::USER_VAR_EVENT => parse_user_var(input, header_ref),      // 14
            LogEventType::FORMAT_DESCRIPTION_EVENT => {   // 15
                let (i, event) = FormatDescriptionEvent::parse(input, header_ref.clone())?;
                /* updating position in context */
                context_ref.borrow_mut().clone().set_log_position_with_offset(header_ref.get_log_pos());

                Ok((
                    i,
                    Event::FormatDescription {
                        event,
                    },
                ))
            },
            LogEventType::XID_EVENT => parse_xid(input, header_ref),           // 16
            LogEventType::BEGIN_LOAD_QUERY_EVENT => parse_begin_load_query(input, header_ref),      // 17
            LogEventType::EXECUTE_LOAD_QUERY_EVENT => parse_execute_load_query(input, header_ref),    // 18
            LogEventType::TABLE_MAP_EVENT => parse_table_map(input, header_ref),     // 19
            // 20, PreGaWriteRowsEvent， unreachable
            // 21, PreGaUpdateRowsEvent， unreachable
            // 22, PreGaDeleteRowsEvent， unreachable
            // 23, WriteRowsEventV1， unreachable
            // 24, UpdateRowsEventV1， unreachable
            // 25, DeleteRowsEventV1， unreachable
            // LogEventType::PRE_GA_WRITE_ROWS_EVENT..=LogEventType::DELETE_ROWS_EVENT_V1 => unreachable!(),

            LogEventType::INCIDENT_EVENT => parse_incident(input, header_ref),      // 26
            LogEventType::HEARTBEAT_LOG_EVENT => parse_heartbeat(input, header_ref),     // 27
            // 28 IgnorableLogEvent
            LogEventType::ROWS_QUERY_LOG_EVENT => parse_row_query(input, header_ref),     // 29
            LogEventType::WRITE_ROWS_EVENT => parse_write_rows_v2(input, header_ref), // 30
            LogEventType::UPDATE_ROWS_EVENT => parse_update_rows_v2(input, header_ref),// 31
            LogEventType::DELETE_ROWS_EVENT => parse_delete_rows_v2(input, header_ref),// 32
            LogEventType::GTID_LOG_EVENT => { // 33
                let (i, event) = GtidLogEvent::parse(input, header_ref.clone())?;
                /* updating position in context */
                context_ref.borrow_mut().clone().set_log_position_with_offset(header_ref.get_log_pos());

                Ok((
                    i,
                    Event::GtidLog {
                        event,
                    },
                ))
            },
            LogEventType::ANONYMOUS_GTID_LOG_EVENT => { // 34
                let (i, event) = AnonymousGtidLogEvent::parse(input, header_ref.clone())?;
                /* updating position in context */
                context_ref.borrow_mut().clone().set_log_position_with_offset(header_ref.get_log_pos());

                Ok((
                    i,
                    Event::AnonymousGtidLog {
                        event,
                    },
                ))
            },
            LogEventType::PREVIOUS_GTIDS_LOG_EVENT => {  // 35
                let (i, event) = PreviousGtidsLogEvent::parse(input, header_ref.clone())?;
                /* updating position in context */
                context_ref.borrow_mut().clone().set_log_position_with_offset(header_ref.get_log_pos());

                Ok((
                    i,
                    Event::PreviousGtidsLog {
                        event,
                    },
                ))
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

impl EventParser {
    pub fn new(event: Event, remain_bytes: Vec<u8>) -> Self {
        Self {
            event,
            remain_bytes,
        }
    }
}
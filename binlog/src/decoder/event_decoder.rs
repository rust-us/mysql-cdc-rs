use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use bytes::Buf;
use nom::bytes::complete::take;
use nom::combinator::map;
use nom::IResult;
use serde::Serialize;
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
use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
use crate::events::protocol::write_rows_v12_event::WriteRowsEvent;

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
    fn decode_with_raw(&mut self, raw: &EventRaw, context: Rc<RefCell<LogContext>>) -> Result<(Event, Vec<u8>), ReError>;

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
    fn decode_with_slice(&mut self, slice: &[u8], header: &Header, context: Rc<RefCell<LogContext>>) -> Result<(Event, Vec<u8>), ReError>;
}

#[derive(Debug, Serialize, Clone)]
pub struct LogEventDecoder {
    //// Gets checksum algorithm type used in a binlog file/ bytes.
    need_le_checksum: bool,

    /// Gets TableMapEvent cache required in row events.
    table_map: HashMap<u64, TableMapEvent>,
}

impl EventDecoder for LogEventDecoder {
    fn decode_with_raw(&mut self, raw: &EventRaw, context: Rc<RefCell<LogContext>>) -> Result<(Event, Vec<u8>), ReError> {
        let header = raw.get_header_ref();
        let slice = raw.get_payload();

        self.decode_with_slice(slice, header.as_ref(), context)
    }

    fn decode_with_slice(&mut self, slice: &[u8], header: &Header, context: Rc<RefCell<LogContext>>) -> Result<(Event, Vec<u8>), ReError> {
         match self.parse_bytes(slice, header, context) {
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
            table_map: HashMap::new(),
        }
    }

    /// 接口应该为私有
    pub fn parse_bytes<'a>(&mut self, slice: &'a [u8], header: &Header,
                           mut context: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Event> {
        let mut cursor =  Cursor::new(slice.clone());

        let b_type = header.event_type;
        let type_ = LogEventType::from(b_type);

        let (remaining_bytes, binlog_event) = match type_ {
            LogEventType::UNKNOWN_EVENT => parse_unknown(slice, &header),
            // 1 START_EVENT_V3事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
            LogEventType::START_EVENT_V3 => {
                unreachable!();
            },
            LogEventType::QUERY_EVENT => {
                let (i, event) = QueryEvent::parse(slice, &header, context.clone())?;
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                // header.putGtid

                Ok((i, Event::Query(event)))
            },
            LogEventType::STOP_EVENT => parse_stop(slice, &header),
            LogEventType::ROTATE_EVENT => {
                parse_rotate(slice, &header)
            },
            LogEventType::INTVAR_EVENT => {
                parse_intvar(slice, &header)
            },
            LogEventType::LOAD_EVENT => parse_load(slice, &header),
            LogEventType::SLAVE_EVENT => parse_slave(slice, &header),
            LogEventType::CREATE_FILE_EVENT => parse_create_file(slice, &header),
            LogEventType::APPEND_BLOCK_EVENT => parse_append_block(slice, &header),  // 9
            LogEventType::EXEC_LOAD_EVENT => parse_exec_load(slice, &header),     // 10
            LogEventType::DELETE_FILE_EVENT => parse_delete_file(slice, &header),   // 11
            LogEventType::NEW_LOAD_EVENT => parse_new_load(slice, &header),      // 12
            LogEventType::RAND_EVENT => parse_rand(slice, &header),          // 13
            LogEventType::USER_VAR_EVENT => parse_user_var(slice, &header),      // 14
            LogEventType::FORMAT_DESCRIPTION_EVENT => {   // 15
                let (i, event) = FormatDescriptionEvent::parse(slice, &header)?;
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                context.borrow_mut().set_format_description(event.clone());

                Ok((i, Event::FormatDescription(event)))
            },
            LogEventType::XID_EVENT => parse_xid(slice, &header),           // 16
            LogEventType::BEGIN_LOAD_QUERY_EVENT => parse_begin_load_query(slice, &header),      // 17
            LogEventType::EXECUTE_LOAD_QUERY_EVENT => parse_execute_load_query(slice, &header),    // 18
            LogEventType::TABLE_MAP_EVENT => {     // 19
                let (i, event) = TableMapEvent::parse(slice, &header, context.clone())?;
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                context.borrow_mut().put_table(event.get_table_id(), event.clone());

                Ok((i, Event::TableMap(event)))
            },
            // 20, PreGaWriteRowsEvent， unreachable
            // 21, PreGaUpdateRowsEvent， unreachable
            // 22, PreGaDeleteRowsEvent， unreachable
            // LogEventType::PRE_GA_WRITE_ROWS_EVENT..=LogEventType::PRE_GA_DELETE_ROWS_EVENT => unreachable!(),


            LogEventType::INCIDENT_EVENT => parse_incident(slice, &header),      // 26
            LogEventType::HEARTBEAT_LOG_EVENT => parse_heartbeat(slice, &header),     // 27
            // 28 IgnorableLogEvent
            LogEventType::ROWS_QUERY_LOG_EVENT => parse_row_query(slice, &header),     // 29

            // Rows events used in MariaDB and MySQL from 5.1.15 to 5.6.
            // 23, LogEventType::WRITE_ROWS_EVENT_V1
            // 24, UpdateRowsEventV1
            // 25, DeleteRowsEventV1
            //
            // MySQL specific events. Rows events used only in MySQL from 5.6 to 8.0.
            LogEventType::WRITE_ROWS_EVENT_V1 | // 23
            LogEventType::WRITE_ROWS_EVENT => { // 30
                let event = WriteRowsEvent::parse(&mut cursor, &self.table_map,
                                                       &header, context.clone());
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                // header.Gtid

                let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
                Ok((i, Event::WriteRows(event.unwrap())))
            },
            LogEventType::UPDATE_ROWS_EVENT_V1 | // 24
            LogEventType::UPDATE_ROWS_EVENT => { // 31
                // parse_update_rows_v2(slice, &header)
                let event = UpdateRowsEvent::parse(&mut cursor, &self.table_map,
                                       &header, context.clone());

                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                // header.Gtid

                let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
                Ok((i, Event::UpdateRows(event.unwrap())))
            },
            LogEventType::DELETE_ROWS_EVENT_V1 | // 25
            LogEventType::DELETE_ROWS_EVENT => { // 32
                parse_delete_rows_v2(slice, &header)
            },

            LogEventType::GTID_LOG_EVENT => { // 33
                let (i, event) = GtidLogEvent::parse(slice, &header)?;
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                // update latest gtid
                // setGtidLogEvent

                Ok((i, Event::GtidLog(event)))
            },
            LogEventType::ANONYMOUS_GTID_LOG_EVENT => { // 34
                let (i, event) = AnonymousGtidLogEvent::parse(slice, &header)?;
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
                // update latest gtid
                // setGtidLogEvent

                Ok((i, Event::AnonymousGtidLog(event)))
            },
            LogEventType::PREVIOUS_GTIDS_LOG_EVENT => {  // 35
                let (i, event) = PreviousGtidsLogEvent::parse(slice, &header)?;
                /* updating position in context */
                context.borrow_mut().set_log_position_with_offset(header.get_log_pos());

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
        }?;

        if let Event::TableMap(e) = &binlog_event {
            self.table_map.insert(e.table_id, e.clone()); //todo: optimize
            // 兼容
            TABLE_MAP_EVENT.lock().unwrap().insert(e.table_id, e.clone());
        }

        Ok((remaining_bytes, binlog_event))
    }
}

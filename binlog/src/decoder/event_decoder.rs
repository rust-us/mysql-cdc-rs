use std::collections::HashMap;
use std::io::Cursor;
use serde::Serialize;
use tracing::error;
use common::err::decode_error::{Needed, ReError};
use crate::alias::mysql::events::gtid_log_event::GtidLogEvent;
use crate::alias::mysql::events::previous_gtids_event::PreviousGtidsLogEvent;
use crate::b_type::LogEventType;
use crate::binlog_server::TABLE_MAP_EVENT;
use crate::decoder::event_decoder_impl::{parse_append_block, parse_begin_load_query, parse_create_file, parse_delete_file, parse_exec_load, parse_execute_load_query, parse_heartbeat, parse_incident, parse_load, parse_new_load, parse_rand, parse_row_query, parse_user_var};
use crate::events::checksum_type::ChecksumType;
use crate::events::declare::log_event::LogEvent;
use crate::events::declare::rows_log_event::RowsLogEvent;

use crate::events::event::Event;
use crate::events::event_header::Header;
use crate::events::event_raw::{EventRaw, HeaderRef};
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::log_position::LogPosition;
use crate::events::protocol::anonymous_gtid_log_event::AnonymousGtidLogEvent;
use crate::events::protocol::delete_rows_v12_event::DeleteRowsEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::ignorable_log_event::IgnorableLogEvent;
use crate::events::protocol::int_var_event::IntVarEvent;
use crate::events::protocol::query_event::QueryEvent;
use crate::events::protocol::rotate_event::RotateEvent;
use crate::events::protocol::slave_event::SlaveEvent;
use crate::events::protocol::stop_event::StopEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::protocol::unknown_event::UnknownEvent;
use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
use crate::events::protocol::v4::start_v3_event::StartV3Event;
use crate::events::protocol::write_rows_v12_event::WriteRowsEvent;
use crate::events::protocol::xid_event::XidLogEvent;

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
    fn decode_with_raw(&mut self, raw: &EventRaw, context: LogContextRef) -> Result<Event, ReError>;

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
    fn parse_event(&mut self, slice: &[u8], header: HeaderRef, context: LogContextRef) -> Result<Event, ReError>;
}

// is EventParser
#[derive(Debug, Serialize, Clone)]
pub struct LogEventDecoder {
    /// Gets checksum algorithm type used in a binlog file.
    pub checksum_type: ChecksumType,

    /// Gets TableMapEvent cache required in row events.
    pub table_map: HashMap<u64, TableMapEvent>,
}

impl EventDecoder for LogEventDecoder {
    #[inline]
    fn decode_with_raw(&mut self, raw: &EventRaw, context: LogContextRef) -> Result<Event, ReError> {
        let header = raw.get_header();
        let slice = raw.get_payload();

        self.event_parse(slice, header, context)
    }

    #[inline]
    fn parse_event(&mut self, slice: &[u8], header: HeaderRef, context: LogContextRef) -> Result<Event, ReError> {
        self.event_parse(slice, header, context)
    }
}

impl LogEventDecoder {
    #[inline]
    pub fn new() -> Self {
        Self {
            checksum_type: ChecksumType::None,
            table_map: HashMap::new(),
        }
    }

    #[inline]
    pub fn new_with_checksum_type(checksum_type: ChecksumType) -> Self {
        Self {
            checksum_type,
            table_map: HashMap::new(),
        }
    }


    /// Parsing and processing of each event
    pub fn event_parse(&mut self, slice: &[u8], mut header: HeaderRef,
                                  mut context: LogContextRef) -> Result<Event, ReError> {
        let checksum_type = &self.checksum_type;
        // Consider verifying checksum
        let mut cursor = match checksum_type {
            ChecksumType::None => Cursor::new(slice.clone()),

            // 此处认为 slice 中不应该包含 crc信息。 暂时在内部处理掉， 后续再同意约束处理
            ChecksumType::Crc32 => Cursor::new(slice.clone()),
            // ChecksumType::Crc32 => Cursor::new(&slice.clone()[0..slice.len() - 4]),
        };

        let b_type = header.borrow().event_type;
        let type_ = LogEventType::from(b_type);

        let has_gtid = context.borrow().get_gtid_set().is_some();
        let binlog_event = match type_ {
            LogEventType::UNKNOWN_EVENT => {
                let event = UnknownEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                /* updating position in context */
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::Unknown(event))
            },

            // 1 START_EVENT_V3事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
            LogEventType::START_EVENT_V3 => {
                let e = StartV3Event::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::StartV3(e))
            },

            LogEventType::QUERY_EVENT => {
                let event = QueryEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                header.borrow_mut().update_gtid(
                    context.borrow().get_gtid_set(),
                    context.borrow().get_gtid_log_event()
                );

                Ok(Event::Query(event))
            },

            LogEventType::STOP_EVENT => {
                let e = StopEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::Stop(e))
            },

            LogEventType::ROTATE_EVENT => {
                let event = RotateEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                // updating new position in context
                context.borrow_mut().set_log_position(LogPosition::new_with_position(&event.get_file_name(), *&event.get_binlog_position()));

                Ok(Event::Rotate(event))
            },

            LogEventType::INTVAR_EVENT => {
                let event = IntVarEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::IntVar(event))
            },

            LogEventType::LOAD_EVENT => {
                let (a, e) = parse_load(slice, header).unwrap();
                Ok(e)
            },

            LogEventType::SLAVE_EVENT => {
                // can never happen (unused event)， also unsupported SLAVE_EVENT
                let e = SlaveEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::Slave(e))
            },

            LogEventType::CREATE_FILE_EVENT => {
                let (a, e) = parse_create_file(slice, header).unwrap();
                Ok(e)
            },
            LogEventType::APPEND_BLOCK_EVENT => {
                let (a, e) = parse_append_block(slice, header).unwrap();
                Ok(e)
            },  // 9
            LogEventType::EXEC_LOAD_EVENT => {
                let (a, e) = parse_exec_load(slice, header).unwrap();
                Ok(e)
            },     // 10
            LogEventType::DELETE_FILE_EVENT => {
                let (a, e) = parse_delete_file(slice, header).unwrap();
                Ok(e)
            },   // 11
            LogEventType::NEW_LOAD_EVENT => {
                let (a, e) = parse_new_load(slice, header).unwrap();
                Ok(e)
            },      // 12
            LogEventType::RAND_EVENT => {   // 13
                let (a, e) = parse_rand(slice, header).unwrap();
                Ok(e)
                // header.put_gtid
            },
            LogEventType::USER_VAR_EVENT => {    // 14
                let (a, e) = parse_user_var(slice, header).unwrap();
                Ok(e)
                // header.put_gtid
            },

            LogEventType::FORMAT_DESCRIPTION_EVENT => {   // 15
                let event = FormatDescriptionEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                context.borrow_mut().set_format_description(event.clone());

                Ok(Event::FormatDescription(event))
            },

            LogEventType::XID_EVENT => { // 16
                let event = XidLogEvent::parse(&mut cursor, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                header.borrow_mut().update_gtid(
                    context.borrow().get_gtid_set(),
                    context.borrow().get_gtid_log_event()
                );

                Ok(Event::XID(event))
            },
            LogEventType::BEGIN_LOAD_QUERY_EVENT => {
                let (a, e) = parse_begin_load_query(slice, header).unwrap();
                Ok(e)
            },      // 17
            LogEventType::EXECUTE_LOAD_QUERY_EVENT => {
                let (a, e) = parse_execute_load_query(slice, header).unwrap();
                Ok(e)
            },    // 18

            LogEventType::TABLE_MAP_EVENT => {     // 19
                let (i, event) = TableMapEvent::parse(slice, header.clone(), context.clone(), None).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                context.borrow_mut().put_table(event.get_table_id(), event.clone());

                Ok(Event::TableMap(event))
            },

            // 20, 21, 22
            LogEventType::PRE_GA_WRITE_ROWS_EVENT |
            LogEventType::PRE_GA_UPDATE_ROWS_EVENT |
            LogEventType::PRE_GA_DELETE_ROWS_EVENT => {
                format!("Skipping unsupported PRE_GA_UPDATE_ROWS_EVENT from: {}.", header.borrow().get_log_pos());

                Ok(Event::IgnorableLogEvent)
            },

            LogEventType::INCIDENT_EVENT => {
                let (a, e) = parse_incident(slice, header).unwrap();
                Ok(e)
            },      // 26
            LogEventType::HEARTBEAT_LOG_EVENT => {
                let (a, e) = parse_heartbeat(slice, header).unwrap();
                Ok(e)
            },     // 27

            LogEventType::IGNORABLE_LOG_EVENT => {    // 28
                // do nothing , just ignore log event
                let event_ignore = IgnorableLogEvent::parse(&mut cursor,
                                                            header.clone(), context.clone(), Some(&self.table_map)).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::IgnorableLogEvent)
            },

            LogEventType::ROWS_QUERY_LOG_EVENT => {   // 29
                let (a, e) = parse_row_query(slice, header).unwrap();
                Ok(e)
                // header.put_gtid
            },

            // Rows events used in MariaDB and MySQL from 5.1.15 to 5.6: LogEventType::WRITE_ROWS_EVENT_V1(23)， UpdateRowsEventV1(24)， DeleteRowsEventV1(25)
            // MySQL specific events. Rows events used only in MySQL from 5.6 to 8.0: WRITE_ROWS_EVENT, UPDATE_ROWS_EVENT, DELETE_ROWS_EVENT
            LogEventType::WRITE_ROWS_EVENT_V1 | // 23
            LogEventType::WRITE_ROWS_EVENT => { // 30
                let mut event = WriteRowsEvent::parse(&mut cursor,
                                                      header.clone(), context.clone(), Some(&self.table_map)).unwrap();

                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                event.fill_assembly_table(context.clone()).unwrap();

                header.borrow_mut().update_gtid(
                    context.borrow().get_gtid_set(),
                    context.borrow().get_gtid_log_event()
                );

                Ok(Event::WriteRows(event))
            },

            LogEventType::UPDATE_ROWS_EVENT_V1 | // 24
            LogEventType::UPDATE_ROWS_EVENT => { // 31
                let mut event = UpdateRowsEvent::parse(&mut cursor,
                                                       header.clone(), context.clone(), Some(&self.table_map)).unwrap();

                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                event.fill_assembly_table(context.clone()).unwrap();
                header.borrow_mut().update_gtid(
                    context.borrow().get_gtid_set(),
                    context.borrow().get_gtid_log_event()
                );

                Ok(Event::UpdateRows(event))
            },

            LogEventType::DELETE_ROWS_EVENT_V1 | // 25
            LogEventType::DELETE_ROWS_EVENT => { // 32
                let mut event = DeleteRowsEvent::parse(&mut cursor,
                                                       header.clone(), context.clone(), Some(&self.table_map)).unwrap();

                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());
                event.fill_assembly_table(context.clone()).unwrap();
                header.borrow_mut().update_gtid(
                    context.borrow().get_gtid_set(),
                    context.borrow().get_gtid_log_event()
                );

                Ok(Event::DeleteRows(event))
            },

            LogEventType::GTID_LOG_EVENT => { // 33
                let event = GtidLogEvent::parse(&mut cursor,
                                                header.clone(), context.clone(), Some(&self.table_map)).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                {
                    if has_gtid {
                        context.borrow_mut().update_gtid_set(event.get_gtid_str());

                        // update latest gtid
                        header.borrow_mut().update_gtid(
                            Some(context.borrow().get_gtid_set().unwrap()),
                            context.borrow().get_gtid_log_event()
                        );
                    }
                }

                // update current gtid event to context
                context.borrow_mut().set_gtid_log_event(event.clone());

                Ok(Event::GtidLog(event))
            },

            LogEventType::ANONYMOUS_GTID_LOG_EVENT => { // 34
                let event = AnonymousGtidLogEvent::parse(&mut cursor,
                                                         header.clone(), context.clone(), Some(&self.table_map)).unwrap();
                let event = event.gtid_event;
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                {
                    if has_gtid {
                        context.borrow_mut().update_gtid_set(event.get_gtid_str());

                        // update latest gtid
                        header.borrow_mut().update_gtid(
                            Some(context.borrow().get_gtid_set().unwrap()),
                            context.borrow().get_gtid_log_event()
                        );
                    }
                }

                context.borrow_mut().set_gtid_log_event(event.clone());

                Ok(Event::AnonymousGtidLog(event))
            },

            LogEventType::PREVIOUS_GTIDS_LOG_EVENT => {  // 35
                let event = PreviousGtidsLogEvent::parse(&mut cursor,
                                                         header.clone(), context.clone(), Some(&self.table_map)).unwrap();
                context.borrow_mut().update_log_position_with_offset(header.borrow().get_log_pos());

                Ok(Event::PreviousGtidsLog(event))
            },

            // TRANSACTION_CONTEXT_EVENT 36
            // VIEW_CHANGE_EVENT  37
            // XA_PREPARE_LOG_EVENT  38
            // PARTIAL_UPDATE_ROWS_EVENT {
            //      header.put_gtid
            //}
            // TRANSACTION_PAYLOAD_EVENT
            // @see https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html#a4a991abea842d4e50cbee0e490c28ceea1b1312ed0f5322b720ab2b957b0e9999
            // HEARTBEAT_LOG_EVENT_V2
            // ENUM_END_EVENT
            t @ _ => {
                let code = t.as_val();

                error!("unexpected event type: {:x}", code);
                return Err(ReError::Incomplete(Needed::InvalidData(
                    format!("unexpected event type: {}", code)
                )));
            }
        };

        match binlog_event {
            Ok(e) => {
                if let Event::FormatDescription(x) = &e {
                    self.checksum_type = x.get_checksum_type();
                }

                if let Event::TableMap(e) = &e {
                    //todo: optimize
                    self.table_map.insert(e.table_id, e.clone());
                    // 兼容
                    TABLE_MAP_EVENT.lock().unwrap().insert(e.table_id, e.clone());
                }

                return Ok(e);
            },
            Err(err) => {
                Err(err)
            }
        }
    }
}

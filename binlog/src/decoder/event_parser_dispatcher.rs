use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;
use nom::bytes::complete::take;
use nom::combinator::map;
use nom::IResult;
use crate::b_type::LogEventType;
use crate::decoder::event_decoder::LogEventDecoder;
use crate::decoder::event_decoder_impl::{parse_append_block, parse_begin_load_query,
                                         parse_create_file, parse_delete_file, parse_exec_load,
                                         parse_execute_load_query, parse_heartbeat, parse_incident,
                                         parse_intvar, parse_load, parse_new_load, parse_rand, parse_row_query,
                                         parse_slave, parse_stop, parse_user_var, parse_xid, TABLE_MAP_EVENT};
use crate::events::checksum_type::ChecksumType;
use crate::events::event::Event;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext};
use crate::events::log_position::LogPosition;
use crate::events::protocol::anonymous_gtid_log_event::AnonymousGtidLogEvent;
use crate::events::protocol::delete_rows_v12_event::DeleteRowsEvent;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::gtid_log_event::GtidLogEvent;
use crate::events::protocol::previous_gtids_event::PreviousGtidsLogEvent;
use crate::events::protocol::query_event::QueryEvent;
use crate::events::protocol::rotate_event::RotateEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::protocol::unknown_event::UnknownEvent;
use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
use crate::events::protocol::v4::start_v3_event::StartV3Event;
use crate::events::protocol::write_rows_v12_event::WriteRowsEvent;

/// Parsing and processing of each event
pub fn event_parse_diapatcher<'a>(event_decoder: &mut LogEventDecoder, slice: &'a [u8], header: &Header,
                       mut context: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Event> {
    let checksum_type = &event_decoder.checksum_type;
    // Consider verifying checksum
    let mut cursor = match checksum_type {
        ChecksumType::None => Cursor::new(slice.clone()),

        // 此处认为 slice 中不应该包含 crc信息。 暂时在内部处理掉， 后续再同意约束处理
        ChecksumType::Crc32 => Cursor::new(slice.clone()),
        // ChecksumType::Crc32 => Cursor::new(&slice.clone()[0..slice.len() - 4]),
    };

    let b_type = header.event_type;
    let type_ = LogEventType::from(b_type);

    let (remaining_bytes, binlog_event) = match type_ {
        LogEventType::UNKNOWN_EVENT => {
            let event = UnknownEvent::parse(&mut cursor, &header, context.clone(), checksum_type).unwrap();

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
            Ok((i, Event::Unknown(event)))
        },
        // 1 START_EVENT_V3事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
        LogEventType::START_EVENT_V3 => {
            let e = StartV3Event::parse(&mut cursor, &header, context.clone(), checksum_type).unwrap();

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
            Ok((i, Event::StartV3(e)))
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
            let event = RotateEvent::parse(&mut cursor, &header, context.clone()).unwrap();
            /* updating position in context */
            context.borrow_mut().set_log_position(LogPosition::new_with_position(&event.get_file_name(), *&event.get_binlog_position()));

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
            Ok((i, Event::Rotate(event)))
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
            let event = FormatDescriptionEvent::parse(&mut cursor, &header, context.clone()).unwrap();
            /* updating position in context */
            context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
            context.borrow_mut().set_format_description(event.clone());

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
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
        // 23, LogEventType::WRITE_ROWS_EVENT_V1， 24, UpdateRowsEventV1， 25, DeleteRowsEventV1
        // MySQL specific events. Rows events used only in MySQL from 5.6 to 8.0.
        LogEventType::WRITE_ROWS_EVENT_V1 | // 23
        LogEventType::WRITE_ROWS_EVENT => { // 30
            let event = WriteRowsEvent::parse(&mut cursor, &event_decoder.table_map,
                                              &header, context.clone());
            /* updating position in context */
            context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
            // header.Gtid

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
            Ok((i, Event::WriteRows(event.unwrap())))
        },
        LogEventType::UPDATE_ROWS_EVENT_V1 | // 24
        LogEventType::UPDATE_ROWS_EVENT => { // 31
            let event = UpdateRowsEvent::parse(&mut cursor, &event_decoder.table_map,
                                               &header, context.clone());

            /* updating position in context */
            context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
            // header.Gtid

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
            Ok((i, Event::UpdateRows(event.unwrap())))
        },
        LogEventType::DELETE_ROWS_EVENT_V1 | // 25
        LogEventType::DELETE_ROWS_EVENT => { // 32
            let event = DeleteRowsEvent::parse(&mut cursor, slice.clone(), &event_decoder.table_map,
                                               &header, context.clone());

            /* updating position in context */
            context.borrow_mut().set_log_position_with_offset(header.get_log_pos());
            // header.Gtid

            let (i, bytes) = map(take(slice.len()), |s: &[u8]| s)(slice)?;
            Ok((i, Event::DeleteRows(event.unwrap())))
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

    if let Event::FormatDescription(x) = &binlog_event {
        event_decoder.checksum_type = x.get_checksum_type();
    }

    if let Event::TableMap(e) = &binlog_event {
        event_decoder.table_map.insert(e.table_id, e.clone()); //todo: optimize
        // 兼容
        TABLE_MAP_EVENT.lock().unwrap().insert(e.table_id, e.clone());
    }

    Ok((remaining_bytes, binlog_event))
}

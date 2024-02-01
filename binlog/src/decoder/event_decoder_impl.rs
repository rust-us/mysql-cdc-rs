use lazy_static::lazy_static;
use nom::{
    bytes::complete::take,
    combinator::map,
    IResult,
    multi::{many0, many1, many_m_n},
    number::complete::{le_u16, le_u32, le_u64, le_u8},
    sequence::tuple,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use crate::{
    events::binlog_event::BinlogEvent,
    events::event_header::Header,
    utils::{extract_string, read_null_term_string, read_variable_len_string},
};
use crate::events::{DupHandlingFlags, EmptyFlags, IncidentEventType, OptFlags, query, UserVarType};
use crate::events::event_raw::HeaderRef;
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

fn extract_many_fields<'a>(
    input: &'a [u8],
    header: HeaderRef,
    num_fields: u32,
    table_name_length: u8,
    schema_length: u8,
) -> IResult<&'a [u8], (Vec<u8>, Vec<String>, String, String, String)> {
    let (i, field_name_lengths) = map(take(num_fields), |s: &[u8]| s.to_vec())(input)?;
    let total_len: u64 = field_name_lengths.iter().sum::<u8>() as u64 + num_fields as u64;
    let (i, raw_field_names) = take(total_len)(i)?;
    let (_, field_names) =
        many_m_n(num_fields as usize, num_fields as usize, read_null_term_string)(raw_field_names)?;
    let (i, table_name) = map(take(table_name_length + 1), |s: &[u8]| extract_string(s))(i)?;
    let (i, schema_name) = map(take(schema_length + 1), |s: &[u8]| extract_string(s))(i)?;
    let (i, file_name) = map(
        take(
            header.borrow_mut().get_event_length() as usize
                - LOG_EVENT_HEADER_LEN as usize
                - 25
                - num_fields as usize
                - total_len as usize
                - table_name_length as usize
                - schema_length as usize
                - 3
                - 4,
        ),
        |s: &[u8]| extract_string(s),
    )(i)?;
    Ok((
        i,
        (
            field_name_lengths,
            field_names,
            table_name,
            schema_name,
            file_name,
        ),
    ))
}

pub fn parse_load<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (
        i,
        (
            thread_id,
            execution_time,
            skip_lines,
            table_name_length,
            schema_length,
            num_fields,
            field_term,
            enclosed_by,
            line_term,
            line_start,
            escaped_by,
        ),
    ) = tuple((
        le_u32, le_u32, le_u32, le_u8, le_u8, le_u32, le_u8, le_u8, le_u8, le_u8, le_u8,
    ))(input)?;
    let (i, opt_flags) = map(le_u8, |flags: u8| OptFlags {
        dump_file: (flags) % 2 == 1,
        opt_enclosed: (flags >> 1) % 2 == 1,
        replace: (flags >> 2) % 2 == 1,
        ignore: (flags >> 3) % 2 == 1,
    })(i)?;
    let (i, empty_flags) = map(le_u8, |flags: u8| EmptyFlags {
        field_term_empty: (flags) % 2 == 1,
        enclosed_empty: (flags >> 1) % 2 == 1,
        line_term_empty: (flags >> 2) % 2 == 1,
        line_start_empty: (flags >> 3) % 2 == 1,
        escape_empty: (flags >> 4) % 2 == 1,
    })(i)?;
    let (i, (field_name_lengths, field_names, table_name, schema_name, file_name)) =
        extract_many_fields(i, header.clone(), num_fields, table_name_length, schema_length)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        BinlogEvent::Load {
            header: Header::copy(header.clone()),
            thread_id,
            execution_time,
            skip_lines,
            table_name_length,
            schema_length,
            num_fields,
            field_term,
            enclosed_by,
            line_term,
            line_start,
            escaped_by,
            opt_flags,
            empty_flags,
            field_name_lengths,
            field_names,
            table_name,
            schema_name,
            file_name,
            checksum,
        },
    ))
}

pub fn parse_file_data<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], (u32, String, u32)> {
    let (i, file_id) = le_u32(input)?;
    let (i, block_data) = map(take(header.borrow().get_event_length() - LOG_EVENT_HEADER_LEN as u32 - 4 - 4), |s: &[u8]| {
        extract_string(s)
    })(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((i, (file_id, block_data, checksum)))
}

pub fn parse_create_file<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, (file_id, block_data, checksum)) = parse_file_data(input, header.clone())?;
    Ok((
        i,
        BinlogEvent::CreateFile {
            header: Header::copy(header),
            file_id,
            block_data,
            checksum,
        },
    ))
}

pub fn parse_append_block<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, (file_id, block_data, checksum)) = parse_file_data(input, header.clone())?;
    Ok((
        i,
        BinlogEvent::AppendBlock {
            header: Header::copy(header),
            file_id,
            block_data,
            checksum,
        },
    ))
}

pub fn parse_exec_load<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    map(
        tuple((le_u16, le_u32)),
        |(file_id, checksum): (u16, u32)| BinlogEvent::ExecLoad {
            header: Header::copy(header.clone()),
            file_id,
            checksum,
        },
    )(input)
}

pub fn parse_delete_file<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    map(
        tuple((le_u16, le_u32)),
        |(file_id, checksum): (u16, u32)| BinlogEvent::DeleteFile {
            header: Header::copy(header.clone()),
            file_id,
            checksum,
        },
    )(input)
}

pub fn extract_from_prev<'a>(input: &'a [u8]) -> IResult<&'a [u8], (u8, String)> {
    let (i, len) = le_u8(input)?;
    map(take(len), move |s| (len, read_variable_len_string(s, len as usize)))(i)
}

pub fn parse_new_load<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, (thread_id, execution_time, skip_lines, table_name_length, schema_length, num_fields)) =
        tuple((le_u32, le_u32, le_u32, le_u8, le_u8, le_u32))(input)?;
    let (i, (field_term_length, field_term)) = extract_from_prev(i)?;
    let (i, (enclosed_by_length, enclosed_by)) = extract_from_prev(i)?;
    let (i, (line_term_length, line_term)) = extract_from_prev(i)?;
    let (i, (line_start_length, line_start)) = extract_from_prev(i)?;
    let (i, (escaped_by_length, escaped_by)) = extract_from_prev(i)?;
    let (i, opt_flags) = map(le_u8, |flags| OptFlags {
        dump_file: (flags >> 0) % 2 == 1,
        opt_enclosed: (flags >> 1) % 2 == 1,
        replace: (flags >> 2) % 2 == 1,
        ignore: (flags >> 3) % 2 == 1,
    })(i)?;
    let (i, (field_name_lengths, field_names, table_name, schema_name, file_name)) =
        extract_many_fields(i, header.clone(), num_fields, table_name_length, schema_length)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        BinlogEvent::NewLoad {
            header: Header::copy(header.clone()),
            thread_id,
            execution_time,
            skip_lines,
            table_name_length,
            schema_length,
            num_fields,
            field_name_lengths,
            field_term,
            enclosed_by_length,
            enclosed_by,
            line_term_length,
            line_term,
            line_start_length,
            line_start,
            escaped_by_length,
            escaped_by,
            opt_flags,
            field_term_length,
            field_names,
            table_name,
            schema_name,
            file_name,
            checksum,
        },
    ))
}

pub fn parse_rand<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, (seed1, seed2, checksum)) = tuple((le_u64, le_u64, le_u32))(input)?;
    Ok((
        i,
        BinlogEvent::Rand {
            header: Header::copy(header.clone()),
            seed1,
            seed2,
            checksum,
        },
    ))
}

//
// pub fn parse_user_var<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
//     let (i, name_length) = le_u32(input)?;
//     let (i, name) = map(take(name_length), |s: &[u8]| {
//         read_variable_len_string(s, name_length as usize)
//     })(i)?;
//     let (i, is_null) = map(le_u8, |v| v == 1)(i)?;
//     if is_null {
//         let (i, checksum) = le_u32(i)?;
//         Ok((
//             i,
//             BinlogEvent::UserVar {
//                 header: Header::copy(header.clone()),
//                 name_length,
//                 name,
//                 is_null,
//                 d_type: None,
//                 charset: None,
//                 value_length: None,
//                 value: None,
//                 flags: None,
//                 checksum,
//             },
//         ))
//     } else {
//         let (i, d_type) = map(le_u8, |v| match v {
//             0 => Some(UserVarType::STRING),
//             1 => Some(UserVarType::REAL),
//             2 => Some(UserVarType::INT),
//             3 => Some(UserVarType::ROW),
//             4 => Some(UserVarType::DECIMAL),
//             5 => Some(UserVarType::VALUE_TYPE_COUNT),
//             _ => Some(UserVarType::Unknown),
//         })(i)?;
//         let (i, charset) = map(le_u32, |v| Some(v))(i)?;
//         let (i, value_length) = le_u32(i)?;
//         let (i, value) = map(take(value_length), |s: &[u8]| Some(s.to_vec()))(i)?;
//         // TODO still don't know wether should take flag or not
//         let (i, flags) = match d_type.clone().unwrap() {
//             UserVarType::INT => {
//                 let (i, flags) = map(le_u8, |v| Some(v))(i)?;
//                 (i, flags)
//             }
//             _ => (i, None),
//         };
//         let (i, checksum) = le_u32(i)?;
//         Ok((
//             i,
//             BinlogEvent::UserVar {
//                 header: Header::copy(header.clone()),
//                 name,
//                 name_length,
//                 is_null,
//                 d_type,
//                 charset,
//                 value_length: Some(value_length),
//                 value,
//                 flags,
//                 checksum,
//             },
//         ))
//     }
// }

pub fn parse_begin_load_query<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, (file_id, block_data, checksum)) = parse_file_data(input, header.clone())?;
    Ok((
        i,
        BinlogEvent::BeginLoadQuery {
            header: Header::copy(header.clone()),
            file_id,
            block_data,
            checksum,
        },
    ))
}

pub fn parse_execute_load_query<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (
        i,
        (
            thread_id,
            execution_time,
            schema_length,
            error_code,
            status_vars_length,
            file_id,
            start_pos,
            end_pos,
        ),
    ) = tuple((
        le_u32, le_u32, le_u8, le_u16, le_u16, le_u32, le_u32, le_u32,
    ))(input)?;
    let (i, dup_handling_flags) = map(le_u8, |flags| match flags {
        0 => DupHandlingFlags::Error,
        1 => DupHandlingFlags::Ignore,
        2 => DupHandlingFlags::Replace,
        _ => unreachable!(),
    })(i)?;
    let (i, raw_vars) = take(status_vars_length)(i)?;
    // replace of parse_status_var_cursor
    let (remain, status_vars) = many0(query::parse_status_var)(raw_vars)?;
    assert_eq!(remain.len(), 0);
    let (i, schema) = map(take(schema_length), |s: &[u8]| {
        String::from_utf8(s[0..schema_length as usize].to_vec()).unwrap()
    })(i)?;
    let (i, _) = take(1usize)(i)?;
    let (i, query) = map(
        take(
            header.borrow().get_event_length() - LOG_EVENT_HEADER_LEN as u32 - 26 - status_vars_length as u32 - schema_length as u32 - 1 - 4,
        ),
        |s: &[u8]| extract_string(s),
    )(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        BinlogEvent::ExecuteLoadQueryEvent {
            header: Header::copy(header),
            thread_id,
            execution_time,
            schema_length,
            error_code,
            status_vars_length,
            file_id,
            start_pos,
            end_pos,
            dup_handling_flags,
            status_vars,
            schema,
            query,
            checksum,
        },
    ))
}

pub fn parse_incident<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, d_type) = map(le_u16, |t| match t {
        0x0000 => IncidentEventType::None,
        0x0001 => IncidentEventType::LostEvents,
        _ => unreachable!(),
    })(input)?;
    let (i, message_length) = le_u8(i)?;
    let (i, message) = map(take(message_length), |s: &[u8]| {
        read_variable_len_string(s, message_length as usize)
    })(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        BinlogEvent::Incident {
            header: Header::copy(header),
            d_type,
            message_length,
            message,
            checksum,
        },
    ))
}

pub fn parse_heartbeat<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, checksum) = le_u32(input)?;
    Ok((i, BinlogEvent::Heartbeat {
        header: Header::copy(header),
        checksum
    }))
}

pub fn parse_row_query<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], BinlogEvent> {
    let (i, length) = le_u8(input)?;
    let (i, query_text) = map(take(length), |s: &[u8]| read_variable_len_string(s, length as usize))(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        BinlogEvent::RowQuery {
            header: Header::copy(header),
            length,
            query_text,
            checksum,
        },
    ))
}

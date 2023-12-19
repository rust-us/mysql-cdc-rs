use lazy_static::lazy_static;
use nom::{
    bytes::complete::{tag, take},
    combinator::map,
    multi::{many0, many1, many_m_n},
    number::complete::{le_i64, le_u16, le_u32, le_u64, le_u8},
    sequence::tuple,
    IResult,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use std::ops::Deref;
use std::rc::Rc;

use crate::{
    mysql::{ColTypes, ColValues},
    utils::{extract_string, int_by_length_encoded, pu64, string_by_fixed_len, string_by_nul_terminated, string_by_variable_len},
    events::event_header::{Header},
    events::event::{Event},
};
use crate::events::{DupHandlingFlags, EmptyFlags, IncidentEventType, IntVarEventType, OptFlags, query, rows, UserVarType};
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

lazy_static! {
    pub static ref TABLE_MAP: Arc<Mutex<HashMap<u64, Vec<ColTypes>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

pub fn parse_unknown<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    map(le_u32, move |checksum: u32| Event::Unknown {
        header: Header::copy(&header),
        checksum,
    })(input)
}

pub fn parse_stop<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, checksum) = le_u32(input)?;
    Ok((i, Event::Stop {
        header: Header::copy(&header),
        checksum
    }))
}

/// 最后一个rotate event用于说明下一个binlog文件。
pub fn parse_rotate<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, position) = le_u64(input)?;
    let str_len = header.event_length - LOG_EVENT_HEADER_LEN as u32 - 8 - 4;
    let (i, next_binlog) = map(take(str_len), |s: &[u8]| string_by_variable_len(s, str_len as usize))(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::Rotate {
            header: Header::copy(&header),
            position,
            next_binlog,
            checksum,
        },
    ))
}

pub fn parse_intvar<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, e_type) = map(le_u8, |t: u8| match t {
        0x00 => IntVarEventType::InvalidIntEvent,
        0x01 => IntVarEventType::LastInsertIdEvent,
        0x02 => IntVarEventType::InsertIdEvent,
        _ => unreachable!(),
    })(input)?;
    let (i, (value, checksum)) = tuple((le_u64, le_u32))(i)?;
    Ok((
        i,
        Event::IntVar {
            header: Header::copy(&header),
            e_type,
            value,
            checksum,
        },
    ))
}

pub fn extract_many_fields<'a>(
    input: &'a [u8],
    header: &Header,
    num_fields: u32,
    table_name_length: u8,
    schema_length: u8,
) -> IResult<&'a [u8], (Vec<u8>, Vec<String>, String, String, String)> {
    let (i, field_name_lengths) = map(take(num_fields), |s: &[u8]| s.to_vec())(input)?;
    let total_len: u64 = field_name_lengths.iter().sum::<u8>() as u64 + num_fields as u64;
    let (i, raw_field_names) = take(total_len)(i)?;
    let (_, field_names) =
        many_m_n(num_fields as usize, num_fields as usize, string_by_nul_terminated)(raw_field_names)?;
    let (i, table_name) = map(take(table_name_length + 1), |s: &[u8]| extract_string(s))(i)?;
    let (i, schema_name) = map(take(schema_length + 1), |s: &[u8]| extract_string(s))(i)?;
    let (i, file_name) = map(
        take(
            header.event_length as usize
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

pub fn parse_load<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
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
        extract_many_fields(i, &header, num_fields, table_name_length, schema_length)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::Load {
            header: Header::copy(&header),
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

pub fn parse_slave<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, checksum) = le_u32(input)?;
    Ok((i, Event::Slave {
        header: Header::copy(&header),
        checksum
    }))
}

pub fn parse_file_data<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], (u32, String, u32)> {
    let (i, file_id) = le_u32(input)?;
    let (i, block_data) = map(take(header.event_length - LOG_EVENT_HEADER_LEN as u32 - 4 - 4), |s: &[u8]| {
        extract_string(s)
    })(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((i, (file_id, block_data, checksum)))
}

pub fn parse_create_file<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (file_id, block_data, checksum)) = parse_file_data(input, &header)?;
    Ok((
        i,
        Event::CreateFile {
            header: Header::copy(&header),
            file_id,
            block_data,
            checksum,
        },
    ))
}

pub fn parse_append_block<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (file_id, block_data, checksum)) = parse_file_data(input, &header)?;
    Ok((
        i,
        Event::AppendBlock {
            header: Header::copy(&header),
            file_id,
            block_data,
            checksum,
        },
    ))
}

pub fn parse_exec_load<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    map(
        tuple((le_u16, le_u32)),
        |(file_id, checksum): (u16, u32)| Event::ExecLoad {
            header: Header::copy(&header),
            file_id,
            checksum,
        },
    )(input)
}

pub fn parse_delete_file<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    map(
        tuple((le_u16, le_u32)),
        |(file_id, checksum): (u16, u32)| Event::DeleteFile {
            header: Header::copy(&header),
            file_id,
            checksum,
        },
    )(input)
}

pub fn extract_from_prev<'a>(input: &'a [u8]) -> IResult<&'a [u8], (u8, String)> {
    let (i, len) = le_u8(input)?;
    map(take(len), move |s| (len, string_by_variable_len(s, len as usize)))(i)
}

pub fn parse_new_load<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
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
        extract_many_fields(i, &header, num_fields, table_name_length, schema_length)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::NewLoad {
            header: Header::copy(&header),
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

pub fn parse_rand<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (seed1, seed2, checksum)) = tuple((le_u64, le_u64, le_u32))(input)?;
    Ok((
        i,
        Event::Rand {
            header: Header::copy(&header),
            seed1,
            seed2,
            checksum,
        },
    ))
}


pub fn parse_user_var<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, name_length) = le_u32(input)?;
    let (i, name) = map(take(name_length), |s: &[u8]| {
        string_by_variable_len(s, name_length as usize)
    })(i)?;
    let (i, is_null) = map(le_u8, |v| v == 1)(i)?;
    if is_null {
        let (i, checksum) = le_u32(i)?;
        Ok((
            i,
            Event::UserVar {
                header: Header::copy(&header),
                name_length,
                name,
                is_null,
                d_type: None,
                charset: None,
                value_length: None,
                value: None,
                flags: None,
                checksum,
            },
        ))
    } else {
        let (i, d_type) = map(le_u8, |v| match v {
            0 => Some(UserVarType::STRING),
            1 => Some(UserVarType::REAL),
            2 => Some(UserVarType::INT),
            3 => Some(UserVarType::ROW),
            4 => Some(UserVarType::DECIMAL),
            5 => Some(UserVarType::VALUE_TYPE_COUNT),
            _ => Some(UserVarType::Unknown),
        })(i)?;
        let (i, charset) = map(le_u32, |v| Some(v))(i)?;
        let (i, value_length) = le_u32(i)?;
        let (i, value) = map(take(value_length), |s: &[u8]| Some(s.to_vec()))(i)?;
        // TODO still don't know wether should take flag or not
        let (i, flags) = match d_type.clone().unwrap() {
            UserVarType::INT => {
                let (i, flags) = map(le_u8, |v| Some(v))(i)?;
                (i, flags)
            }
            _ => (i, None),
        };
        let (i, checksum) = le_u32(i)?;
        Ok((
            i,
            Event::UserVar {
                header: Header::copy(&header),
                name,
                name_length,
                is_null,
                d_type,
                charset,
                value_length: Some(value_length),
                value,
                flags,
                checksum,
            },
        ))
    }
}

pub fn parse_xid<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (xid, checksum)) = tuple((le_u64, le_u32))(input)?;
    Ok((
        i,
        Event::XID {
            header: Header::copy(&header),
            xid,
            checksum,
        },
    ))
}

pub fn parse_begin_load_query<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (file_id, block_data, checksum)) = parse_file_data(input, &header)?;
    Ok((
        i,
        Event::BeginLoadQuery {
            header: Header::copy(&header),
            file_id,
            block_data,
            checksum,
        },
    ))
}

pub fn parse_execute_load_query<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
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
    let (remain, status_vars) = many0(query::parse_status_var)(raw_vars)?;
    assert_eq!(remain.len(), 0);
    let (i, schema) = map(take(schema_length), |s: &[u8]| {
        String::from_utf8(s[0..schema_length as usize].to_vec()).unwrap()
    })(i)?;
    let (i, _) = take(1usize)(i)?;
    let (i, query) = map(
        take(
            header.event_length - LOG_EVENT_HEADER_LEN as u32 - 26 - status_vars_length as u32 - schema_length as u32 - 1 - 4,
        ),
        |s: &[u8]| extract_string(s),
    )(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::ExecuteLoadQueryEvent {
            header: Header::copy(&header),
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

pub fn parse_incident<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, d_type) = map(le_u16, |t| match t {
        0x0000 => IncidentEventType::None,
        0x0001 => IncidentEventType::LostEvents,
        _ => unreachable!(),
    })(input)?;
    let (i, message_length) = le_u8(i)?;
    let (i, message) = map(take(message_length), |s: &[u8]| {
        string_by_variable_len(s, message_length as usize)
    })(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::Incident {
            header: Header::copy(&header),
            d_type,
            message_length,
            message,
            checksum,
        },
    ))
}

pub fn parse_heartbeat<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, checksum) = le_u32(input)?;
    Ok((i, Event::Heartbeat {
        header: Header::copy(&header),
        checksum
    }))
}

pub fn parse_row_query<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, length) = le_u8(input)?;
    let (i, query_text) = map(take(length), |s: &[u8]| string_by_variable_len(s, length as usize))(i)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::RowQuery {
            header: Header::copy(&header),
            length,
            query_text,
            checksum,
        },
    ))
}

pub fn parse_part_row_event<'a>(
    input: &'a [u8],
) -> IResult<&'a [u8], (u64, rows::Flags, u16, Vec<rows::ExtraData>, (usize, u64))> {
    let (i, table_id): (&'a [u8], u64) = map(take(6usize), |id_raw: &[u8]| {
        let mut filled = id_raw.to_vec();
        filled.extend(vec![0, 0]);
        pu64(&filled).unwrap().1
    })(input)?;
    let (i, flags) = map(le_u16, |flag: u16| rows::Flags {
        end_of_stmt: (flag >> 0) % 2 == 1,
        foreign_key_checks: (flag >> 1) % 2 == 0,
        unique_key_checks: (flag >> 2) % 2 == 0,
        has_columns: (flag >> 3) % 2 == 0,
    })(i)?;
    let (i, extra_data_len) = le_u16(i)?;
    assert!(extra_data_len >= 2);
    let (i, extra_data) = match extra_data_len {
        2 => (i, vec![]),
        _ => many1(rows::parse_extra_data)(i)?,
    };

    // parse body
    let (i, (encode_len, column_count)) = int_by_length_encoded(i)?;
    Ok((
        i,
        (
            table_id,
            flags,
            extra_data_len,
            extra_data,
            (encode_len, column_count),
        ),
    ))
}

pub fn parse_row<'a>(
    input: &'a [u8],
    init_idx: usize,
    col_def: &Vec<ColTypes>,
) -> IResult<&'a [u8], Vec<ColValues>> {
    let mut index = if input.len() != 0 { init_idx } else { 0 };
    let mut ret = vec![];
    for col in col_def {
        let (_, (offset, col_val)) = col.parse(&input[index..])?;
        ret.push(col_val);
        index += offset;
    }
    Ok((&input[index..], ret))
}

pub fn parse_write_rows_v2<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (table_id, flags, extra_data_len, extra_data, (encode_len, column_count))) =
        parse_part_row_event(input)?;
    let bit_len = (column_count + 7) / 8;
    let (i, inserted_image_bits) = map(take(bit_len), |s: &[u8]| s.to_vec())(i)?;
    let (i, col_data) = take(
        header.event_length
            - LOG_EVENT_HEADER_LEN as u32
            - 6
            - 2
            - extra_data_len as u32
            - encode_len as u32
            - ((column_count as u32 + 7) / 8)
            - 4,
    )(i)?;
    let (_, rows) = many1(|s| {
        parse_row(
            s,
            bit_len as usize,
            TABLE_MAP.lock().unwrap().get(&table_id).unwrap(),
        )
    })(col_data)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::WriteRowsV2 {
            header: Header::copy(&header),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            column_count,
            inserted_image_bits,
            rows,
            checksum,
        },
    ))
}

pub fn parse_delete_rows_v2<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (table_id, flags, extra_data_len, extra_data, (encode_len, column_count))) =
        parse_part_row_event(input)?;

    let bit_len = (column_count + 7) / 8;
    let (i, deleted_image_bits) = map(take(bit_len), |s: &[u8]| s.to_vec())(i)?;
    let (i, col_data) = take(
        header.event_length
            - LOG_EVENT_HEADER_LEN as u32
            - 6
            - 2
            - extra_data_len as u32
            - encode_len as u32
            - ((column_count as u32 + 7) / 8)
            - 4,
    )(i)?;
    let (_, rows) = many1(|s| {
        parse_row(
            s,
            bit_len as usize,
            TABLE_MAP.lock().unwrap().get(&table_id).unwrap(),
        )
    })(col_data)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::DeleteRowsV2 {
            header: Header::copy(&header),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            column_count,
            deleted_image_bits,
            rows,
            checksum,
        },
    ))
}

pub fn parse_update_rows_v2<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
    let (i, (table_id, flags, extra_data_len, extra_data, (encode_len, column_count))) =
        parse_part_row_event(input)?;

    let bit_len = (column_count + 7) / 8;
    let (i, before_image_bits) = map(take(bit_len), |s: &[u8]| s.to_vec())(i)?;
    let (i, after_image_bits) = map(take(bit_len), |s: &[u8]| s.to_vec())(i)?;
    // TODO I still don't know is it right or not :(
    let (i, col_data) = take(
        header.event_length as u64
            - LOG_EVENT_HEADER_LEN as u64
            - 6
            - 2
            - extra_data_len as u64
            - encode_len as u64
            - bit_len * 2
            - 4,
    )(i)?;
    let (_, rows) = many1(|s| {
        parse_row(
            s,
            bit_len as usize,
            TABLE_MAP.lock().unwrap().get(&table_id).unwrap(),
        )
    })(col_data)?;
    let (i, checksum) = le_u32(i)?;
    Ok((
        i,
        Event::UpdateRowsV2 {
            header: Header::copy(&header),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            column_count,
            before_image_bits,
            after_image_bits,
            rows,
            checksum,
        },
    ))
}
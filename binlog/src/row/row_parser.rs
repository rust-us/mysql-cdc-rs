use std::collections::HashMap;
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt};
use tracing::error;
use common::column::column_type::ColumnType;
use common::column::column_value::ColumnValue;
use common::err::decode_error::ReError;
use crate::column::column_parser::{parse_bit, parse_blob, parse_date, parse_date_time, parse_date_time2, parse_string, parse_time, parse_time2, parse_timestamp, parse_timestamp2, parse_year};
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::{ExtraData, ExtraDataFormat, Flags, Payload};
use crate::binlog_server::TABLE_MAP_EVENT;
use crate::events::declare::log_event::EXTRA_ROW_INFO_HDR_BYTES;
use crate::events::protocol::format_description_log_event::ROWS_HEADER_LEN_V2;
use crate::row::actual_string_type::get_actual_string_type;
use crate::row::decimal::parse_decimal;
use crate::row::row_data::{RowData, UpdateRowData};
use crate::row::rows::{ExtraDataType, RowEventVersion};
use crate::utils::{read_bitmap_little_endian, read_len_enc_num_with_cursor, read_string, u8_to_bool};

pub const TABLE_MAP_NOT_FOUND: &str =
    "No preceding TableMapEvent event was found for the row event. \
You possibly started replication in the middle of logical event group.";


/// 解析 row 数据的 Post-Header 信息
///
/// # Arguments
///
/// * `cursor`:
/// * `post_header_len`:
///
/// returns:
/// Result<(u64, u16, u16, Vec<ExtraData, Global>, usize, RowEventVersion), ReError> ,
/// values is :
/// (table_id, flags, extra_data_len, extra_data, columns_number, version)
///
pub fn parse_head(cursor: &mut Cursor<&[u8]>, post_header_len:u8)
                  -> Result<(u64, u16, u16, Vec<ExtraData>, usize, RowEventVersion), ReError> {
    let table_id = match post_header_len as u32 {
        6 => {
            // Master is of an intermediate source tree before 5.1.4. Id is 4 bytes
            cursor.read_u32::<LittleEndian>()? as u64
        },
        _ => {
            // RW_FLAGS_OFFSET
            cursor.read_u48::<LittleEndian>()? as u64
        }
    };

    let flags = cursor.read_u16::<LittleEndian>()?;
    let f = Flags::from(flags);

    let (extra_data_length, extra_data, version) = if post_header_len == ROWS_HEADER_LEN_V2 {
        let extra_data_length = cursor.read_u16::<LittleEndian>()?;
        assert!(extra_data_length >= 2);

        let header_len:usize = extra_data_length as usize - 2usize;

        let extra_data = match header_len {
            /* ExtraDataType::RW_V_EXTRAINFO_TAG */
            0 => vec![],
            _ => {
                let mut extra_data_vec = vec![0; header_len];
                cursor.read_exact(&mut extra_data_vec)?;
                let mut extra_data_cursor = Cursor::new(extra_data_vec.as_slice());

                let mut v = vec![];
                while extra_data_cursor.position() < extra_data_cursor.get_ref().len() as u64 {
                    let extra = parse_extra_data(&mut extra_data_cursor)?;
                    v.push(extra);
                }
                v
            },
        };

        (extra_data_length, extra_data, RowEventVersion::V2)

        // let skip = extra_data_length as i64 - 2;
        // // skip
        // cursor.seek(SeekFrom::Current(skip))?;
    } else {
        (0, vec![], RowEventVersion::V1)
    };

    let (_, columns_number) = read_len_enc_num_with_cursor(cursor)?;

    Ok((table_id, flags, extra_data_length, extra_data, columns_number as usize, version))
}

/// Parsing row based events.
/// See <a href="https://mariadb.com/kb/en/library/rows_event_v1/">MariaDB rows version 1</a>
/// See <a href="https://dev.mysql.com/doc/internals/en/rows-event.html#write-rows-eventv2">MySQL rows version 1/2</a>
/// See <a href="https://github.com/shyiko/mysql-binlog-connector-java">AbstractRowsEventDataDeserializer</a>
pub fn parse_row_data_list(
    cursor: &mut Cursor<&[u8]>,
    table_map: &HashMap<u64, TableMapEvent>,
    table_id: u64,
    columns_present: &Vec<bool>) -> Result<Vec<RowData>, ReError> {

    let tme = TABLE_MAP_EVENT.lock().unwrap();
    let table = match table_map.get(&table_id) {
        Some(x) => x,
        None => {
            // 兼容
            match tme.get(&table_id) {
                Some(y) => y,
                None => {
                    return Err(ReError::String(TABLE_MAP_NOT_FOUND.to_string()))
                },
            }
        },
    };

    // let columns_present = u8_to_bool(image_bits);
    let cells_included = get_bits_number(&columns_present);
    let mut rows = Vec::new();

    while cursor.position() < cursor.get_ref().len() as u64 {
        let row_result = parse_row(cursor, table, &columns_present, cells_included);

        if let Err(error) = &row_result {
            if let ReError::IoError(io_error) = error {
                // failed to fill whole buffer, 文件读到了最后
                if let ErrorKind::UnexpectedEof = io_error.kind() {
                    break;
                } else {
                    println!("{:?}", error);
                }
            } else {
                println!("{:?}", error);
            }
        }

        rows.push(row_result.unwrap());
    }

    Ok(rows)
}


pub fn parse_update_row_data_list(
    cursor: &mut Cursor<&[u8]>,
    table_map: &HashMap<u64, TableMapEvent>,
    table_id: u64,
    before_image: &Vec<bool>,
    after_image: &Vec<bool>) -> Result<Vec<UpdateRowData>, ReError> {

    let tme = TABLE_MAP_EVENT.lock().unwrap();
    let table = match table_map.get(&table_id) {
        Some(x) => x,
        None => {
            // 兼容
            match tme.get(&table_id) {
                Some(y) => y,
                None => {
                    return Err(ReError::String(TABLE_MAP_NOT_FOUND.to_string()))
                },
            }
        },
    };

    let cells_included_before_update = get_bits_number(before_image);
    let cells_included_after_update = get_bits_number(after_image);
    let mut rows = Vec::new();

    while cursor.position() < cursor.get_ref().len() as u64 {
        let row_before_update_content = parse_row(
            cursor,
            table,
            before_image,
            cells_included_before_update,
        )?;

        let row_after_update_content = parse_row(
            cursor,
            table,
            after_image,
            cells_included_after_update,
        )?;

        rows.push(UpdateRowData::new(row_before_update_content, row_after_update_content));
    }

    Ok(rows)
}

fn parse_row(
    cursor: &mut Cursor<&[u8]>,
    table_map: &TableMapEvent,
    columns_present: &Vec<bool>,
    cells_included: usize) -> Result<RowData, ReError> {

    let column_types = table_map.get_column_types();
    let mut row = Vec::with_capacity(column_types.len());
    let null_bitmap = read_bitmap_little_endian(cursor, cells_included)?;

    let mut skipped_columns = 0;
    for i in 0..column_types.len() {
        // Data is missing if binlog_row_image != full
        if !columns_present[i] {
            skipped_columns += 1;
            row.push(None);
        }
        // Column is present and has null value
        else if null_bitmap[i - skipped_columns] {
            row.push(None);
        }

        // Column has data
        else {
            let mut column_type = column_types[i];
            let mut metadata = table_map.column_metadata[i];

            if ColumnType::try_from(column_type).unwrap() == ColumnType::String {
                get_actual_string_type(&mut column_type, &mut metadata);
            }

            row.push(Some(parse_cell(cursor, column_type, metadata)?));
        }
    }

    Ok(RowData::new_with_cells(row))
}

/// Gets number of bits set in a bitmap.
fn get_bits_number(bitmap: &Vec<bool>) -> usize {
    bitmap.iter().filter(|&x| *x == true).count()
}

fn parse_cell(
    cursor: &mut Cursor<&[u8]>,
    column_type: u8,
    metadata: u16) -> Result<ColumnValue, ReError> {

    let value = match ColumnType::try_from(column_type).unwrap() {
        /* Numeric types. The only place where numbers can be negative */
        ColumnType::Tiny => ColumnValue::TinyInt(cursor.read_u8()?),
        ColumnType::Short => ColumnValue::SmallInt(cursor.read_u16::<LittleEndian>()?),
        ColumnType::Int24 => ColumnValue::MediumInt(cursor.read_u24::<LittleEndian>()?),
        ColumnType::Long => ColumnValue::Int(cursor.read_u32::<LittleEndian>()?),
        ColumnType::LongLong => ColumnValue::BigInt(cursor.read_u64::<LittleEndian>()?),
        ColumnType::Float => ColumnValue::Float(cursor.read_f32::<LittleEndian>()?),
        ColumnType::Double => ColumnValue::Double(cursor.read_f64::<LittleEndian>()?),
        ColumnType::NewDecimal => ColumnValue::Decimal(parse_decimal(cursor, metadata)?),
        /* String types, includes varchar, varbinary & fixed char, binary */
        ColumnType::String => ColumnValue::String(parse_string(cursor, metadata)?),
        ColumnType::VarChar => ColumnValue::String(parse_string(cursor, metadata)?),
        ColumnType::VarString => ColumnValue::String(parse_string(cursor, metadata)?),
        /* BIT, ENUM, SET types */
        ColumnType::Bit => ColumnValue::Bit(parse_bit(cursor, metadata)?),
        ColumnType::Enum => {
            ColumnValue::Enum(cursor.read_uint::<LittleEndian>(metadata as usize)? as u32)
        }
        ColumnType::Set => {
            ColumnValue::Set(cursor.read_uint::<LittleEndian>(metadata as usize)? as u64)
        }
        /* Blob types. MariaDB always creates BLOB for first three */
        ColumnType::TinyBlob => ColumnValue::Blob(parse_blob(cursor, metadata)?),
        ColumnType::MediumBlob => ColumnValue::Blob(parse_blob(cursor, metadata)?),
        ColumnType::LongBlob => ColumnValue::Blob(parse_blob(cursor, metadata)?),
        ColumnType::Blob => ColumnValue::Blob(parse_blob(cursor, metadata)?),
        /* Date and time types */
        ColumnType::Year => ColumnValue::Year(parse_year(cursor, metadata)?),
        ColumnType::Date => ColumnValue::Date(parse_date(cursor, metadata)?),
        // Older versions of MySQL.
        ColumnType::Time => ColumnValue::Time(parse_time(cursor, metadata)?),
        ColumnType::Timestamp => ColumnValue::Timestamp(parse_timestamp(cursor, metadata)?),
        ColumnType::DateTime => ColumnValue::DateTime(parse_date_time(cursor, metadata)?),
        // MySQL 5.6.4+ types. Supported from MariaDB 10.1.2.
        ColumnType::Time2 => ColumnValue::Time(parse_time2(cursor, metadata)?),
        ColumnType::Timestamp2 => ColumnValue::Timestamp(parse_timestamp2(cursor, metadata)?),
        ColumnType::DateTime2 => ColumnValue::DateTime(parse_date_time2(cursor, metadata)?),
        /* MySQL-specific data types */
        ColumnType::Geometry => ColumnValue::Blob(parse_blob(cursor, metadata)?),
        ColumnType::Json => ColumnValue::Blob(parse_blob(cursor, metadata)?),
        _ => {
            return Err(ReError::String(format!(
                "Parsing column type {:?} is not supported",
                ColumnType::try_from(column_type).unwrap()
            )))
        }
    };

    Ok(value)
}

fn parse_extra_data<'a>(cursor: &mut Cursor<&[u8]>) -> Result<ExtraData, ReError> {
    let dt = cursor.read_u8()?;
    let d_type = match dt {
        0x00 => ExtraDataType::RW_V_EXTRAINFO_TAG,
        _ => {
            error!("unknown extra data type {}", dt);
            unreachable!()
        }
    };
    let check_len = cursor.read_u8()?;
    let val = check_len - EXTRA_ROW_INFO_HDR_BYTES;

    let fmt = cursor.read_u8()?;
    assert_eq!(fmt, val); // EXTRA_ROW_INFO_FORMAT_OFFSET
    let extra_data_format = match fmt {
        0x00 => ExtraDataFormat::NDB,
        0x40 => ExtraDataFormat::OPEN1,
        0x41 => ExtraDataFormat::OPEN2,
        0xff => ExtraDataFormat::MULTI,
        _ => {
            error!("unknown extract data format {}", fmt);
            unreachable!()
        }
    };

    let payload = read_string(cursor, check_len as usize)?;

    Ok(ExtraData {
            d_type,
            data: Payload::ExtraDataInfo {
                length: check_len,
                format: extra_data_format,
                payload,
            },
        },
    )
}

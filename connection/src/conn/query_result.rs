use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};

use binlog::utils::read_len_enc_num_with_cursor;
use common::binlog::column::column::SrcColumn;
use common::binlog::column::column_type::SrcColumnType;
use common::binlog::column::column_value;
use common::binlog::column::column_value::SrcColumnValue;
use common::binlog::row::row::Row;
use common::err::decode_error::ReError;
use common::err::CResult;

use crate::conn::connection::Connection;
use crate::declar::capability_flags;
use crate::packet::end_of_file_packet::EndOfFilePacket;
use crate::packet::result_set_column_packet::ResultSetColumnPacket;
use crate::packet::result_set_row_packet::ResultSetRowPacket;

const TIMESTAMP_WITH_MILLS_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";
const TIME_FORMAT_WITH_MILLS_FORMAT: &str = "%H:%M:%S%.f";

const DATE_FORMAT: &str = "%Y-%m-%d";

#[derive(Debug)]
pub struct StreamQueryResult<'a> {
    conn: &'a mut Connection,
    columns: Arc<[SrcColumn]>,
    has_results: bool,
}

impl StreamQueryResult<'_> {
    pub(crate) fn new(conn: &mut Connection, columns: Arc<[SrcColumn]>) -> StreamQueryResult {
        let has_results = columns.len() > 0;
        StreamQueryResult {
            conn,
            columns,
            has_results,
        }
    }

    /// 返回结果集的column
    pub fn columns(&self) -> &Arc<[SrcColumn]> {
        &self.columns
    }
}

impl Iterator for StreamQueryResult<'_> {
    type Item = CResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_results {
            return None;
        }

        let (packet, _) = match self
            .conn
            .read_packet_with_check("Query result row load error.")
        {
            Ok(packet) => packet,
            Err(err) => {
                return Some(Err(ReError::MysqlQueryErr(format!(
                    "Query error. msg:{:?}",
                    err
                ))))
            }
        };

        if EndOfFilePacket::is_eof(packet.as_slice()) {
            self.has_results = false;
            return None;
        }
        let row = match ResultSetRowPacket::parse(&packet) {
            Ok(row) => row,
            Err(err) => {
                return Some(Err(ReError::MysqlQueryErr(format!(
                    "Query result row parse error. msg:{:?}",
                    err
                ))))
            }
        };
        Some(parse_row(row, self.columns.clone()))
    }
}

/// drop时需要将结果读完
impl Drop for StreamQueryResult<'_> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

pub fn read_column_set(conn: &mut Connection) -> CResult<Vec<SrcColumn>> {
    let (packet, _) = conn.read_packet_with_check("Query result column load error.")?;

    let mut cursor = Cursor::new(packet.as_slice());
    let column_count = read_len_enc_num_with_cursor(&mut cursor)?.1;
    let mut columns: Vec<SrcColumn> = Vec::with_capacity(column_count as usize);
    for _ in 0..column_count {
        let (packet, _) = conn.read_packet_with_check("Query result column load error.")?;
        let column = ResultSetColumnPacket::parse(packet.as_slice())?;

        columns.push(parse_column(column)?);
    }

    if !conn.has_capability(capability_flags::CLIENT_DEPRECATE_EOF) {
        // 丢弃 eof packet
        let (_, _) = conn.read_packet_with_check("Query result eof load error.")?;
    }

    Ok(columns)
}

fn parse_column(column: ResultSetColumnPacket) -> CResult<SrcColumn> {
    let column_type = match SrcColumnType::try_from(column.column_type) {
        Ok(src_column) => src_column,
        Err(err) => {
            return Err(ReError::MysqlQueryErr(format!(
                "Can not parse column type. msg:{:?}",
                err
            )))
        }
    };
    Ok(SrcColumn::new(column_type)
        .with_schema(column.schema.as_bytes())
        .with_table(column.table.as_bytes())
        .with_org_table(column.org_table.as_bytes())
        .with_name(column.name.as_bytes())
        .with_org_name(column.org_name.as_bytes())
        .with_character_set(column.character_set)
        .with_column_length(column.column_length)
        .with_flags(column.flags)
        .with_decimals(column.decimals))
}

fn parse_row(row: ResultSetRowPacket, columns: Arc<[SrcColumn]>) -> CResult<Row> {
    let mut values = Vec::with_capacity(columns.len());

    for index in 0..columns.len() {
        let value = row.cells.get(index).unwrap_or(&None);
        let column = columns.get(index);
        if column.is_none() {
            continue;
        }
        values.push(parse_text_value_by_type(
            &value,
            &column.unwrap().column_type(),
        )?);
    }

    Ok(Row::new_row(values, columns.clone()))
}

/// 将query结果的value值按照column类型转换为ColumnValue
fn parse_text_value_by_type(
    ori_value: &Option<String>,
    column_type: &SrcColumnType,
) -> CResult<Option<SrcColumnValue>> {
    if ori_value.is_none() {
        return Ok(None);
    }
    let ori_value = ori_value.clone().unwrap();
    let value = match column_type {
        SrcColumnType::Tiny => SrcColumnValue::TinyInt(parse_string_to_num::<u8>(&ori_value)?),
        SrcColumnType::Short => SrcColumnValue::SmallInt(parse_string_to_num::<u16>(&ori_value)?),
        SrcColumnType::Int24 => SrcColumnValue::MediumInt(parse_string_to_num::<u32>(&ori_value)?),
        SrcColumnType::Long => SrcColumnValue::Int(parse_string_to_num::<u32>(&ori_value)?),
        SrcColumnType::LongLong => SrcColumnValue::BigInt(parse_string_to_num::<u64>(&ori_value)?),
        SrcColumnType::Float => SrcColumnValue::Float(parse_string_to_num::<f32>(&ori_value)?),
        SrcColumnType::Double => SrcColumnValue::Double(parse_string_to_num::<f64>(&ori_value)?),
        SrcColumnType::Decimal |
        SrcColumnType::NewDecimal => SrcColumnValue::Decimal(ori_value.clone()),
        SrcColumnType::VarString | SrcColumnType::VarChar | SrcColumnType::String => {
            SrcColumnValue::String(ori_value.clone())
        }
        // BIT
        // ENUM
        // SET
        SrcColumnType::TinyBlob
        | SrcColumnType::MediumBlob
        | SrcColumnType::LongBlob
        | SrcColumnType::Blob => SrcColumnValue::Blob(ori_value.into_bytes()),
        SrcColumnType::Year => SrcColumnValue::Year(parse_string_to_num::<u16>(&ori_value)?),
        SrcColumnType::Date | SrcColumnType::NewDate => {
            let date = parse_date(&ori_value)?;
            SrcColumnValue::Date(column_value::Date {
                year: date.year() as u16,
                month: date.month() as u8,
                day: date.day() as u8,
            })
        }
        SrcColumnType::Time | SrcColumnType::Time2 => {
            let time = parse_time(&ori_value)?;
            SrcColumnValue::Time(column_value::Time {
                hour: time.hour() as i16, // Signed value from -838 to 838
                minute: time.minute() as u8,
                second: time.second() as u8,
                millis: time.nanosecond() / 1_000_000,
            })
        }
        SrcColumnType::Timestamp | SrcColumnType::Timestamp2 => {
            let date_time = parse_timestamp(&ori_value)?;
            SrcColumnValue::Timestamp(Local.from_utc_datetime(&date_time).timestamp() as u64)
        }
        SrcColumnType::DateTime | SrcColumnType::DateTime2 => {
            let date_time = parse_timestamp(&ori_value)?;
            SrcColumnValue::DateTime(column_value::DateTime {
                year: date_time.year() as u16,
                month: date_time.month() as u8,
                day: date_time.day() as u8,
                hour: date_time.hour() as u8,
                minute: date_time.minute() as u8,
                second: date_time.second() as u8,
                millis: date_time.timestamp_subsec_millis(),
            })
        }
        SrcColumnType::Geometry => SrcColumnValue::Blob(ori_value.into_bytes()),
        // Json
        SrcColumnType::Null => return Ok(None),
        SrcColumnType::Bool => SrcColumnValue::TinyInt(parse_string_to_num::<u8>(&ori_value)?),

        // 其余的类型保留二进制原始数据
        _ => SrcColumnValue::Blob(ori_value.into_bytes()),
    };
    Ok(Some(value))
}

fn parse_string_to_num<T: FromStr>(value: &String) -> CResult<T> {
    match value.parse::<T>() {
        Ok(num) => Ok(num),
        Err(err) => Err(ReError::MysqlQueryErr(format!(
            "Can not parse value:{{{value}}} to number"
        ))),
    }
}

fn parse_timestamp(value: &String) -> CResult<NaiveDateTime> {
    match NaiveDateTime::parse_from_str(value, TIMESTAMP_WITH_MILLS_FORMAT) {
        Ok(time) => Ok(time),
        Err(err) => Err(ReError::MysqlQueryErr(format!(
            "Can not parse timestamp, value:{{{value}}}, format:{{{TIMESTAMP_WITH_MILLS_FORMAT}}}"
        ))),
    }
}

fn parse_date(value: &String) -> CResult<NaiveDate> {
    match NaiveDate::parse_from_str(value, DATE_FORMAT) {
        Ok(time) => Ok(time),
        Err(err) => Err(ReError::MysqlQueryErr(format!(
            "Can not parse date, value:{{{value}}}, format:{{{DATE_FORMAT}}}"
        ))),
    }
}

fn parse_time(value: &String) -> CResult<NaiveTime> {
    match NaiveTime::parse_from_str(value, TIME_FORMAT_WITH_MILLS_FORMAT) {
        Ok(time) => Ok(time),
        Err(err) => Err(ReError::MysqlQueryErr(format!(
            "Can not parse time, value:{{{value}}}, format:{{{TIME_FORMAT_WITH_MILLS_FORMAT}}}"
        ))),
    }
}

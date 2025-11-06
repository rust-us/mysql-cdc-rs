use std::io::Cursor;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use common::binlog::column::column_value::{Date, DateTime, Time};
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;

/// Decoder for YEAR type
pub struct YearDecoder;

impl YearDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for YearDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let year = 1900 + cursor.read_u8()? as u16;
        Ok(ColumnValue::Year(year))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Year as u8
    }

    fn type_name(&self) -> &'static str {
        "YEAR"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(1)
    }
}

/// Decoder for DATE type
pub struct DateDecoder;

impl DateDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for DateDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u24::<LittleEndian>()?;

        // Bits 1-5 store the day. Bits 6-9 store the month. The remaining bits store the year.
        let day = value % (1 << 5);
        let month = (value >> 5) % (1 << 4);
        let year = value >> 9;

        let date = Date {
            year: year as u16,
            month: month as u8,
            day: day as u8,
        };

        Ok(ColumnValue::Date(date))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Date as u8
    }

    fn type_name(&self) -> &'static str {
        "DATE"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(3)
    }
}

/// Decoder for TIME type
pub struct TimeDecoder;

impl TimeDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for TimeDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let mut value = (cursor.read_i24::<LittleEndian>()? << 8) >> 8;

        if value < 0 {
            return Err(ReError::String(
                "Parsing negative TIME values is not supported in this version".to_string(),
            ));
        }

        let second = value % 100;
        value = value / 100;
        let minute = value % 100;
        value = value / 100;
        let hour = value;

        let time = Time {
            hour: hour as i16,
            minute: minute as u8,
            second: second as u8,
            millis: 0,
        };

        Ok(ColumnValue::Time(time))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Time as u8
    }

    fn type_name(&self) -> &'static str {
        "TIME"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(3)
    }
}

/// Decoder for TIME2 type (MySQL 5.6+)
pub struct Time2Decoder;

impl Time2Decoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for Time2Decoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u24::<BigEndian>()?;
        let millis = parse_fractional_part(cursor, metadata.metadata)? / 1000;

        let negative = ((value >> 23) & 1) == 0;
        if negative {
            return Err(ReError::String(
                "Parsing negative TIME values is not supported in this version".to_string(),
            ));
        }

        // 1 bit sign. 1 bit unused. 10 bits hour. 6 bits minute. 6 bits second.
        let hour = (value >> 12) % (1 << 10);
        let minute = (value >> 6) % (1 << 6);
        let second = value % (1 << 6);

        let time = Time {
            hour: hour as i16,
            minute: minute as u8,
            second: second as u8,
            millis: millis as u32,
        };

        Ok(ColumnValue::Time(time))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Time2 as u8
    }

    fn type_name(&self) -> &'static str {
        "TIME2"
    }
}

/// Decoder for DATETIME type
pub struct DateTimeDecoder;

impl DateTimeDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for DateTimeDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let mut value = cursor.read_u64::<LittleEndian>()?;
        let second = value % 100;
        value = value / 100;
        let minute = value % 100;
        value = value / 100;
        let hour = value % 100;
        value = value / 100;
        let day = value % 100;
        value = value / 100;
        let month = value % 100;
        value = value / 100;
        let year = value;

        let datetime = DateTime {
            year: year as u16,
            month: month as u8,
            day: day as u8,
            hour: hour as u8,
            minute: minute as u8,
            second: second as u8,
            millis: 0,
        };

        Ok(ColumnValue::DateTime(datetime))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::DateTime as u8
    }

    fn type_name(&self) -> &'static str {
        "DATETIME"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(8)
    }
}

/// Decoder for DATETIME2 type (MySQL 5.6+)
pub struct DateTime2Decoder;

impl DateTime2Decoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for DateTime2Decoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_uint::<BigEndian>(5)?;
        let millis = parse_fractional_part(cursor, metadata.metadata)? / 1000;

        // 1 bit sign(always true). 17 bits year*13+month. 5 bits day. 5 bits hour. 6 bits minute. 6 bits second.
        let year_month = (value >> 22) % (1 << 17);
        let year = year_month / 13;
        let month = year_month % 13;
        let day = (value >> 17) % (1 << 5);
        let hour = (value >> 12) % (1 << 5);
        let minute = (value >> 6) % (1 << 6);
        let second = value % (1 << 6);

        let datetime = DateTime {
            year: year as u16,
            month: month as u8,
            day: day as u8,
            hour: hour as u8,
            minute: minute as u8,
            second: second as u8,
            millis: millis as u32,
        };

        Ok(ColumnValue::DateTime(datetime))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::DateTime2 as u8
    }

    fn type_name(&self) -> &'static str {
        "DATETIME2"
    }
}

/// Decoder for TIMESTAMP type
pub struct TimestampDecoder;

impl TimestampDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for TimestampDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let seconds = cursor.read_u32::<LittleEndian>()? as u64;
        Ok(ColumnValue::Timestamp(seconds * 1000))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Timestamp as u8
    }

    fn type_name(&self) -> &'static str {
        "TIMESTAMP"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(4)
    }
}

/// Decoder for TIMESTAMP2 type (MySQL 5.6+)
pub struct Timestamp2Decoder;

impl Timestamp2Decoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for Timestamp2Decoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let seconds = cursor.read_u32::<BigEndian>()? as u64;
        let millisecond = parse_fractional_part(cursor, metadata.metadata)? / 1000;
        let timestamp = seconds * 1000 + millisecond;
        Ok(ColumnValue::Timestamp(timestamp))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Timestamp2 as u8
    }

    fn type_name(&self) -> &'static str {
        "TIMESTAMP2"
    }
}

fn parse_fractional_part(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<u64, ReError> {
    let length = (metadata + 1) / 2;
    if length == 0 {
        return Ok(0);
    }

    let fraction = cursor.read_uint::<BigEndian>(length as usize)?;
    Ok(fraction * u64::pow(100, 3 - length as u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_year_decoder() {
        let decoder = YearDecoder::new();
        let data = vec![121u8]; // 1900 + 121 = 2021
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Year as u8, 0);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        assert_eq!(result, ColumnValue::Year(2021));
    }

    #[test]
    fn test_timestamp_decoder() {
        let decoder = TimestampDecoder::new();
        let data = vec![0x60, 0x9A, 0x5E, 0x60]; // Unix timestamp in little-endian
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Timestamp as u8, 0);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        if let ColumnValue::Timestamp(ts) = result {
            assert!(ts > 0);
        } else {
            panic!("Expected timestamp value");
        }
    }
}
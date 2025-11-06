use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;
use crate::row::decimal::parse_decimal;

/// Decoder for TINYINT type
pub struct TinyIntDecoder;

impl TinyIntDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for TinyIntDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u8()?;
        if metadata.unsigned {
            Ok(ColumnValue::UTinyInt(value))
        } else {
            Ok(ColumnValue::TinyInt(value as i8))
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Tiny as u8
    }

    fn type_name(&self) -> &'static str {
        "TINYINT"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(1)
    }
}

/// Decoder for SMALLINT type
pub struct SmallIntDecoder;

impl SmallIntDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for SmallIntDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u16::<LittleEndian>()?;
        if metadata.unsigned {
            Ok(ColumnValue::USmallInt(value))
        } else {
            Ok(ColumnValue::SmallInt(value as i16))
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Short as u8
    }

    fn type_name(&self) -> &'static str {
        "SMALLINT"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(2)
    }
}

/// Decoder for MEDIUMINT type
pub struct MediumIntDecoder;

impl MediumIntDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for MediumIntDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u24::<LittleEndian>()?;
        if metadata.unsigned {
            Ok(ColumnValue::UMediumInt(value))
        } else {
            Ok(ColumnValue::MediumInt(value as i32))
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Int24 as u8
    }

    fn type_name(&self) -> &'static str {
        "MEDIUMINT"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(3)
    }
}

/// Decoder for INT type
pub struct IntDecoder;

impl IntDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for IntDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u32::<LittleEndian>()?;
        if metadata.unsigned {
            Ok(ColumnValue::UInt(value))
        } else {
            Ok(ColumnValue::Int(value as i32))
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Long as u8
    }

    fn type_name(&self) -> &'static str {
        "INT"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(4)
    }
}

/// Decoder for BIGINT type
pub struct BigIntDecoder;

impl BigIntDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for BigIntDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_u64::<LittleEndian>()?;
        if metadata.unsigned {
            Ok(ColumnValue::UBigInt(value))
        } else {
            Ok(ColumnValue::BigInt(value as i64))
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::LongLong as u8
    }

    fn type_name(&self) -> &'static str {
        "BIGINT"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(8)
    }
}

/// Decoder for FLOAT type
pub struct FloatDecoder;

impl FloatDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for FloatDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_f32::<LittleEndian>()?;
        Ok(ColumnValue::Float(value))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Float as u8
    }

    fn type_name(&self) -> &'static str {
        "FLOAT"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(4)
    }
}

/// Decoder for DOUBLE type
pub struct DoubleDecoder;

impl DoubleDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for DoubleDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_f64::<LittleEndian>()?;
        Ok(ColumnValue::Double(value))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Double as u8
    }

    fn type_name(&self) -> &'static str {
        "DOUBLE"
    }

    fn expected_size(&self, _metadata: &ColumnMetadata) -> Option<usize> {
        Some(8)
    }
}

/// Decoder for DECIMAL/NEWDECIMAL type
pub struct DecimalDecoder;

impl DecimalDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for DecimalDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let decimal_str = parse_decimal(cursor, metadata.metadata)?;
        Ok(ColumnValue::Decimal(decimal_str))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::NewDecimal as u8
    }

    fn type_name(&self) -> &'static str {
        "DECIMAL"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_tinyint_decoder() {
        let decoder = TinyIntDecoder::new();
        let data = vec![42u8];
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Tiny as u8, 0);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        assert_eq!(result, ColumnValue::TinyInt(42));
    }

    #[test]
    fn test_unsigned_tinyint_decoder() {
        let decoder = TinyIntDecoder::new();
        let data = vec![200u8];
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Tiny as u8, 0).with_unsigned(true);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        assert_eq!(result, ColumnValue::UTinyInt(200));
    }
}
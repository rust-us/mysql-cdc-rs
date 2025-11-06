use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;
use crate::column::charset::CharsetConverter;
use crate::utils::read_string;

/// Decoder for VARCHAR type with charset support
pub struct VarCharDecoder {
    charset_converter: Option<std::sync::Arc<std::sync::Mutex<CharsetConverter>>>,
}

impl VarCharDecoder {
    pub fn new() -> Self {
        Self {
            charset_converter: Some(std::sync::Arc::new(std::sync::Mutex::new(CharsetConverter::new("utf8mb4")))),
        }
    }

    pub fn with_charset_converter(mut self, converter: CharsetConverter) -> Self {
        self.charset_converter = Some(std::sync::Arc::new(std::sync::Mutex::new(converter)));
        self
    }
}

impl TypeDecoder for VarCharDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = if metadata.metadata < 256 {
            cursor.read_u8()? as usize
        } else {
            cursor.read_u16::<LittleEndian>()? as usize
        };
        
        // Read raw bytes
        let mut bytes = vec![0u8; length];
        cursor.read_exact(&mut bytes)?;
        
        // Convert using charset converter if available
        let string_data = if let Some(ref converter) = self.charset_converter {
            let mut converter_guard = converter.lock()
                .map_err(|e| ReError::String(format!("Failed to lock charset converter: {}", e)))?;
            converter_guard.convert_string(&bytes, metadata.charset)?
        } else {
            // Fallback to UTF-8
            String::from_utf8(bytes)
                .map_err(|e| ReError::String(format!("UTF-8 conversion error: {}", e)))?
        };
        
        Ok(ColumnValue::VarChar(string_data))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::VarChar as u8
    }

    fn type_name(&self) -> &'static str {
        "VARCHAR"
    }
}

/// Decoder for STRING type (CHAR and fixed-length strings)
pub struct StringDecoder;

impl StringDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for StringDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = if metadata.metadata < 256 {
            cursor.read_u8()? as usize
        } else {
            cursor.read_u16::<LittleEndian>()? as usize
        };
        
        let string_data = read_string(cursor, length)?;
        Ok(ColumnValue::Char(string_data))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::String as u8
    }

    fn type_name(&self) -> &'static str {
        "CHAR"
    }
}

/// Decoder for VAR_STRING type
pub struct VarStringDecoder;

impl VarStringDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for VarStringDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = if metadata.metadata < 256 {
            cursor.read_u8()? as usize
        } else {
            cursor.read_u16::<LittleEndian>()? as usize
        };
        
        let string_data = read_string(cursor, length)?;
        Ok(ColumnValue::VarChar(string_data))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::VarString as u8
    }

    fn type_name(&self) -> &'static str {
        "VAR_STRING"
    }
}

/// Decoder for ENUM type
pub struct EnumDecoder;

impl EnumDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for EnumDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as u32;
        Ok(ColumnValue::Enum(value))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Enum as u8
    }

    fn type_name(&self) -> &'static str {
        "ENUM"
    }
}

/// Decoder for SET type
pub struct SetDecoder;

impl SetDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for SetDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let value = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as u64;
        Ok(ColumnValue::Set(value))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Set as u8
    }

    fn type_name(&self) -> &'static str {
        "SET"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_varchar_decoder_short() {
        let decoder = VarCharDecoder::new();
        let mut data = vec![5u8]; // Length = 5
        data.extend_from_slice(b"hello");
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::VarChar as u8, 100); // metadata < 256
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        assert_eq!(result, ColumnValue::VarChar("hello".to_string()));
    }

    #[test]
    fn test_enum_decoder() {
        let decoder = EnumDecoder::new();
        let data = vec![42u8, 0u8]; // 2-byte enum value = 42
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Enum as u8, 2);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        assert_eq!(result, ColumnValue::Enum(42));
    }
}
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;

/// Decoder for BLOB type
pub struct BlobDecoder;

impl BlobDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for BlobDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        Ok(ColumnValue::Blob(vec))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Blob as u8
    }

    fn type_name(&self) -> &'static str {
        "BLOB"
    }
}

/// Decoder for TINYBLOB type
pub struct TinyBlobDecoder;

impl TinyBlobDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for TinyBlobDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        Ok(ColumnValue::TinyBlob(vec))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::TinyBlob as u8
    }

    fn type_name(&self) -> &'static str {
        "TINYBLOB"
    }
}

/// Decoder for MEDIUMBLOB type
pub struct MediumBlobDecoder;

impl MediumBlobDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for MediumBlobDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        Ok(ColumnValue::MediumBlob(vec))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::MediumBlob as u8
    }

    fn type_name(&self) -> &'static str {
        "MEDIUMBLOB"
    }
}

/// Decoder for LONGBLOB type
pub struct LongBlobDecoder;

impl LongBlobDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for LongBlobDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        Ok(ColumnValue::LongBlob(vec))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::LongBlob as u8
    }

    fn type_name(&self) -> &'static str {
        "LONGBLOB"
    }
}

/// Decoder for GEOMETRY type
pub struct GeometryDecoder;

impl GeometryDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for GeometryDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        Ok(ColumnValue::Geometry(vec))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Geometry as u8
    }

    fn type_name(&self) -> &'static str {
        "GEOMETRY"
    }
}

/// Decoder for JSON type (MySQL 5.7+)
pub struct JsonDecoder;

impl JsonDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for JsonDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        
        // Parse JSON from binary format
        // For now, we'll store as raw bytes and parse later
        // In a full implementation, this would parse the MySQL JSON binary format
        let json_str = String::from_utf8(vec)
            .map_err(|e| ReError::String(format!("Invalid UTF-8 in JSON: {}", e)))?;
        
        let json_value: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| ReError::String(format!("Invalid JSON: {}", e)))?;
        
        Ok(ColumnValue::Json(json_value))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Json as u8
    }

    fn type_name(&self) -> &'static str {
        "JSON"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_blob_decoder() {
        let decoder = BlobDecoder::new();
        let mut data = vec![5u8, 0u8, 0u8, 0u8]; // Length = 5 (little-endian)
        data.extend_from_slice(b"hello");
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Blob as u8, 4); // 4-byte length
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        assert_eq!(result, ColumnValue::Blob(b"hello".to_vec()));
    }

    #[test]
    fn test_json_decoder() {
        let decoder = JsonDecoder::new();
        let json_str = r#"{"key": "value"}"#;
        let mut data = vec![json_str.len() as u8, 0u8, 0u8, 0u8]; // Length (little-endian)
        data.extend_from_slice(json_str.as_bytes());
        let mut cursor = Cursor::new(data.as_slice());
        let metadata = ColumnMetadata::new(SrcColumnType::Json as u8, 4);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        if let ColumnValue::Json(json_val) = result {
            assert_eq!(json_val["key"], "value");
        } else {
            panic!("Expected JSON value");
        }
    }
}
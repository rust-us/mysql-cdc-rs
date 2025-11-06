use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;

/// Enhanced JSON decoder with structured data access
pub struct JsonDecoder;

impl JsonDecoder {
    pub fn new() -> Self {
        Self
    }

    /// Parse MySQL's binary JSON format
    fn parse_mysql_json_binary(&self, data: &[u8]) -> Result<serde_json::Value, ReError> {
        if data.is_empty() {
            return Ok(serde_json::Value::Null);
        }

        // MySQL JSON binary format parsing
        // This is a simplified implementation - the actual MySQL JSON binary format is complex
        // For a complete implementation, you would need to handle:
        // - Type markers
        // - Offset tables
        // - Inline values vs. referenced values
        // - Various data type encodings

        let mut cursor = Cursor::new(data);
        
        // Read the type marker (first byte)
        let type_marker = cursor.read_u8()?;
        
        match type_marker {
            0x00 => Ok(serde_json::Value::Null),
            0x01 => {
                // Boolean false
                Ok(serde_json::Value::Bool(false))
            }
            0x02 => {
                // Boolean true
                Ok(serde_json::Value::Bool(true))
            }
            0x03 => {
                // 16-bit signed integer
                let value = cursor.read_i16::<LittleEndian>()?;
                Ok(serde_json::Value::Number(serde_json::Number::from(value)))
            }
            0x04 => {
                // 32-bit signed integer
                let value = cursor.read_i32::<LittleEndian>()?;
                Ok(serde_json::Value::Number(serde_json::Number::from(value)))
            }
            0x05 => {
                // 64-bit signed integer
                let value = cursor.read_i64::<LittleEndian>()?;
                Ok(serde_json::Value::Number(serde_json::Number::from(value)))
            }
            0x06 => {
                // 64-bit unsigned integer
                let value = cursor.read_u64::<LittleEndian>()?;
                Ok(serde_json::Value::Number(serde_json::Number::from(value)))
            }
            0x07 => {
                // Double
                let value = cursor.read_f64::<LittleEndian>()?;
                Ok(serde_json::Number::from_f64(value)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null))
            }
            0x0C => {
                // String - read length and then string data
                let length = self.read_variable_length(&mut cursor)?;
                let mut string_data = vec![0u8; length];
                cursor.read_exact(&mut string_data)?;
                let string_value = String::from_utf8(string_data)
                    .map_err(|e| ReError::String(format!("Invalid UTF-8 in JSON string: {}", e)))?;
                Ok(serde_json::Value::String(string_value))
            }
            0x0F => {
                // Object
                self.parse_json_object(&mut cursor)
            }
            0x10 => {
                // Array
                self.parse_json_array(&mut cursor)
            }
            _ => {
                // Fallback: try to parse as UTF-8 string
                let string_data = String::from_utf8(data.to_vec())
                    .map_err(|e| ReError::String(format!("Unknown JSON type marker {} and invalid UTF-8: {}", type_marker, e)))?;
                
                serde_json::from_str(&string_data)
                    .map_err(|e| ReError::String(format!("Failed to parse JSON string: {}", e)))
            }
        }
    }

    fn read_variable_length(&self, cursor: &mut Cursor<&[u8]>) -> Result<usize, ReError> {
        // Read variable-length integer (simplified implementation)
        let first_byte = cursor.read_u8()?;
        
        if first_byte < 0x80 {
            Ok(first_byte as usize)
        } else if first_byte < 0xC0 {
            let second_byte = cursor.read_u8()?;
            Ok(((first_byte as usize & 0x3F) << 8) | (second_byte as usize))
        } else {
            // For longer lengths, read more bytes
            let length_bytes = (first_byte & 0x3F) as usize;
            let mut length = 0usize;
            for _ in 0..length_bytes {
                length = (length << 8) | (cursor.read_u8()? as usize);
            }
            Ok(length)
        }
    }

    fn parse_json_object(&self, cursor: &mut Cursor<&[u8]>) -> Result<serde_json::Value, ReError> {
        let element_count = self.read_variable_length(cursor)?;
        let _size = self.read_variable_length(cursor)?;
        
        let mut object = serde_json::Map::new();
        
        for _ in 0..element_count {
            // Read key length and key
            let key_length = self.read_variable_length(cursor)?;
            let mut key_data = vec![0u8; key_length];
            cursor.read_exact(&mut key_data)?;
            let key = String::from_utf8(key_data)
                .map_err(|e| ReError::String(format!("Invalid UTF-8 in JSON key: {}", e)))?;
            
            // Read value type and value
            let value_type = cursor.read_u8()?;
            let value = match value_type {
                0x00 => serde_json::Value::Null,
                0x01 => serde_json::Value::Bool(false),
                0x02 => serde_json::Value::Bool(true),
                0x0C => {
                    let value_length = self.read_variable_length(cursor)?;
                    let mut value_data = vec![0u8; value_length];
                    cursor.read_exact(&mut value_data)?;
                    let string_value = String::from_utf8(value_data)
                        .map_err(|e| ReError::String(format!("Invalid UTF-8 in JSON value: {}", e)))?;
                    serde_json::Value::String(string_value)
                }
                _ => {
                    // For other types, recursively parse
                    return Err(ReError::String(format!("Unsupported JSON value type: {}", value_type)));
                }
            };
            
            object.insert(key, value);
        }
        
        Ok(serde_json::Value::Object(object))
    }

    fn parse_json_array(&self, cursor: &mut Cursor<&[u8]>) -> Result<serde_json::Value, ReError> {
        let element_count = self.read_variable_length(cursor)?;
        let _size = self.read_variable_length(cursor)?;
        
        let mut array = Vec::new();
        
        for _ in 0..element_count {
            let value_type = cursor.read_u8()?;
            let value = match value_type {
                0x00 => serde_json::Value::Null,
                0x01 => serde_json::Value::Bool(false),
                0x02 => serde_json::Value::Bool(true),
                0x0C => {
                    let value_length = self.read_variable_length(cursor)?;
                    let mut value_data = vec![0u8; value_length];
                    cursor.read_exact(&mut value_data)?;
                    let string_value = String::from_utf8(value_data)
                        .map_err(|e| ReError::String(format!("Invalid UTF-8 in JSON array value: {}", e)))?;
                    serde_json::Value::String(string_value)
                }
                _ => {
                    return Err(ReError::String(format!("Unsupported JSON array value type: {}", value_type)));
                }
            };
            
            array.push(value);
        }
        
        Ok(serde_json::Value::Array(array))
    }
}

impl TypeDecoder for JsonDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = cursor.read_uint::<LittleEndian>(metadata.metadata as usize)? as usize;
        let mut vec = vec![0; length];
        cursor.read_exact(&mut vec)?;
        
        // Try to parse as MySQL binary JSON format first
        match self.parse_mysql_json_binary(&vec) {
            Ok(json_value) => Ok(ColumnValue::Json(json_value)),
            Err(_) => {
                // Fallback: try to parse as UTF-8 JSON string
                let json_str = String::from_utf8(vec)
                    .map_err(|e| ReError::String(format!("Invalid UTF-8 in JSON: {}", e)))?;
                
                let json_value: serde_json::Value = serde_json::from_str(&json_str)
                    .map_err(|e| ReError::String(format!("Invalid JSON: {}", e)))?;
                
                Ok(ColumnValue::Json(json_value))
            }
        }
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Json as u8
    }

    fn type_name(&self) -> &'static str {
        "JSON"
    }
}

/// JSON utility functions for structured data access
pub struct JsonUtils;

impl JsonUtils {
    /// Extract a value from JSON by path (e.g., "user.name" or "items[0].id")
    pub fn extract_by_path<'a>(json: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;
        
        for part in parts {
            if let Some(array_access) = Self::parse_array_access(part) {
                let (key, index) = array_access;
                current = current.get(key)?;
                current = current.get(index)?;
            } else {
                current = current.get(part)?;
            }
        }
        
        Some(current)
    }

    /// Parse array access notation like "items[0]" -> ("items", 0)
    fn parse_array_access<'a>(part: &'a str) -> Option<(&'a str, usize)> {
        if let Some(bracket_start) = part.find('[') {
            if let Some(bracket_end) = part.find(']') {
                let key = &part[..bracket_start];
                let index_str = &part[bracket_start + 1..bracket_end];
                if let Ok(index) = index_str.parse::<usize>() {
                    return Some((key, index));
                }
            }
        }
        None
    }

    /// Convert JSON value to a specific type
    pub fn extract_as_string(json: &serde_json::Value) -> Option<String> {
        match json {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }

    pub fn extract_as_i64(json: &serde_json::Value) -> Option<i64> {
        match json {
            serde_json::Value::Number(n) => n.as_i64(),
            serde_json::Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn extract_as_f64(json: &serde_json::Value) -> Option<f64> {
        match json {
            serde_json::Value::Number(n) => n.as_f64(),
            serde_json::Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get all keys from a JSON object
    pub fn get_object_keys(json: &serde_json::Value) -> Vec<String> {
        match json {
            serde_json::Value::Object(obj) => obj.keys().cloned().collect(),
            _ => Vec::new(),
        }
    }

    /// Get array length
    pub fn get_array_length(json: &serde_json::Value) -> Option<usize> {
        match json {
            serde_json::Value::Array(arr) => Some(arr.len()),
            _ => None,
        }
    }

    /// Check if JSON contains a specific path
    pub fn has_path(json: &serde_json::Value, path: &str) -> bool {
        Self::extract_by_path(json, path).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_utils_extract_by_path() {
        let json: serde_json::Value = serde_json::from_str(r#"
        {
            "user": {
                "name": "John",
                "age": 30
            },
            "items": [
                {"id": 1, "name": "item1"},
                {"id": 2, "name": "item2"}
            ]
        }
        "#).unwrap();

        assert_eq!(
            JsonUtils::extract_by_path(&json, "user.name"),
            Some(&serde_json::Value::String("John".to_string()))
        );

        assert_eq!(
            JsonUtils::extract_as_i64(JsonUtils::extract_by_path(&json, "user.age").unwrap()),
            Some(30)
        );

        assert!(JsonUtils::has_path(&json, "user.name"));
        assert!(!JsonUtils::has_path(&json, "user.email"));
    }

    #[test]
    fn test_json_utils_object_keys() {
        let json: serde_json::Value = serde_json::from_str(r#"{"a": 1, "b": 2, "c": 3}"#).unwrap();
        let mut keys = JsonUtils::get_object_keys(&json);
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }
}
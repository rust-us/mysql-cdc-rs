use serde::Serialize;
use std::collections::HashMap;
use common::binlog::column::column_value::{Date, DateTime, Time};

/// Unified column value type that combines SrcColumnValue functionality
/// with enhanced type support and extensibility
#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum ColumnValue {
    // Null value
    Null,
    
    // Numeric types
    TinyInt(i8),
    SmallInt(i16),
    MediumInt(i32),
    Int(i32),
    BigInt(i64),
    
    // Unsigned numeric types
    UTinyInt(u8),
    USmallInt(u16),
    UMediumInt(u32),
    UInt(u32),
    UBigInt(u64),
    
    // Floating point types
    Float(f32),
    Double(f64),
    Decimal(String),
    
    // String types
    Char(String),
    VarChar(String),
    Binary(Vec<u8>),
    VarBinary(Vec<u8>),
    
    // Text and blob types
    TinyText(String),
    Text(String),
    MediumText(String),
    LongText(String),
    TinyBlob(Vec<u8>),
    Blob(Vec<u8>),
    MediumBlob(Vec<u8>),
    LongBlob(Vec<u8>),
    
    // Bit type
    Bit(Vec<bool>),
    
    // Enum and Set types
    Enum(u32),
    Set(u64),
    
    // Date and time types
    Year(u16),
    Date(Date),
    Time(Time),
    DateTime(DateTime),
    Timestamp(u64), // millis from unix time
    
    // JSON type (MySQL 5.7+)
    Json(serde_json::Value),
    
    // Geometry type
    Geometry(Vec<u8>),
    
    // Custom/Extended types for future extensibility
    Custom {
        type_name: String,
        data: Vec<u8>,
        metadata: HashMap<String, String>,
    },
}

impl ColumnValue {
    /// Get the type name as a string
    pub fn type_name(&self) -> String {
        match self {
            ColumnValue::Null => "NULL".to_string(),
            ColumnValue::TinyInt(_) => "TINYINT".to_string(),
            ColumnValue::SmallInt(_) => "SMALLINT".to_string(),
            ColumnValue::MediumInt(_) => "MEDIUMINT".to_string(),
            ColumnValue::Int(_) => "INT".to_string(),
            ColumnValue::BigInt(_) => "BIGINT".to_string(),
            ColumnValue::UTinyInt(_) => "TINYINT UNSIGNED".to_string(),
            ColumnValue::USmallInt(_) => "SMALLINT UNSIGNED".to_string(),
            ColumnValue::UMediumInt(_) => "MEDIUMINT UNSIGNED".to_string(),
            ColumnValue::UInt(_) => "INT UNSIGNED".to_string(),
            ColumnValue::UBigInt(_) => "BIGINT UNSIGNED".to_string(),
            ColumnValue::Float(_) => "FLOAT".to_string(),
            ColumnValue::Double(_) => "DOUBLE".to_string(),
            ColumnValue::Decimal(_) => "DECIMAL".to_string(),
            ColumnValue::Char(_) => "CHAR".to_string(),
            ColumnValue::VarChar(_) => "VARCHAR".to_string(),
            ColumnValue::Binary(_) => "BINARY".to_string(),
            ColumnValue::VarBinary(_) => "VARBINARY".to_string(),
            ColumnValue::TinyText(_) => "TINYTEXT".to_string(),
            ColumnValue::Text(_) => "TEXT".to_string(),
            ColumnValue::MediumText(_) => "MEDIUMTEXT".to_string(),
            ColumnValue::LongText(_) => "LONGTEXT".to_string(),
            ColumnValue::TinyBlob(_) => "TINYBLOB".to_string(),
            ColumnValue::Blob(_) => "BLOB".to_string(),
            ColumnValue::MediumBlob(_) => "MEDIUMBLOB".to_string(),
            ColumnValue::LongBlob(_) => "LONGBLOB".to_string(),
            ColumnValue::Bit(_) => "BIT".to_string(),
            ColumnValue::Enum(_) => "ENUM".to_string(),
            ColumnValue::Set(_) => "SET".to_string(),
            ColumnValue::Year(_) => "YEAR".to_string(),
            ColumnValue::Date(_) => "DATE".to_string(),
            ColumnValue::Time(_) => "TIME".to_string(),
            ColumnValue::DateTime(_) => "DATETIME".to_string(),
            ColumnValue::Timestamp(_) => "TIMESTAMP".to_string(),
            ColumnValue::Json(_) => "JSON".to_string(),
            ColumnValue::Geometry(_) => "GEOMETRY".to_string(),
            ColumnValue::Custom { type_name, .. } => type_name.clone(),
        }
    }

    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }

    /// Get the size in bytes (approximate for complex types)
    pub fn size_bytes(&self) -> usize {
        match self {
            ColumnValue::Null => 0,
            ColumnValue::TinyInt(_) | ColumnValue::UTinyInt(_) => 1,
            ColumnValue::SmallInt(_) | ColumnValue::USmallInt(_) => 2,
            ColumnValue::MediumInt(_) | ColumnValue::UMediumInt(_) => 3,
            ColumnValue::Int(_) | ColumnValue::UInt(_) | ColumnValue::Float(_) => 4,
            ColumnValue::BigInt(_) | ColumnValue::UBigInt(_) | ColumnValue::Double(_) | ColumnValue::Timestamp(_) => 8,
            ColumnValue::Year(_) => 2,
            ColumnValue::Date(_) => 3,
            ColumnValue::Time(_) => 3,
            ColumnValue::DateTime(_) => 8,
            ColumnValue::Decimal(s) => s.len(),
            ColumnValue::Char(s) | ColumnValue::VarChar(s) => s.len(),
            ColumnValue::TinyText(s) | ColumnValue::Text(s) | ColumnValue::MediumText(s) | ColumnValue::LongText(s) => s.len(),
            ColumnValue::Binary(b) | ColumnValue::VarBinary(b) => b.len(),
            ColumnValue::TinyBlob(b) | ColumnValue::Blob(b) | ColumnValue::MediumBlob(b) | ColumnValue::LongBlob(b) => b.len(),
            ColumnValue::Geometry(b) => b.len(),
            ColumnValue::Bit(bits) => (bits.len() + 7) / 8,
            ColumnValue::Enum(_) => 4,
            ColumnValue::Set(_) => 8,
            ColumnValue::Json(v) => v.to_string().len(),
            ColumnValue::Custom { data, .. } => data.len(),
        }
    }
}

// Conversion from legacy SrcColumnValue to new ColumnValue
impl From<common::binlog::column::column_value::SrcColumnValue> for ColumnValue {
    fn from(src: common::binlog::column::column_value::SrcColumnValue) -> Self {
        use common::binlog::column::column_value::SrcColumnValue;
        
        match src {
            SrcColumnValue::TinyInt(v) => ColumnValue::UTinyInt(v),
            SrcColumnValue::SmallInt(v) => ColumnValue::USmallInt(v),
            SrcColumnValue::MediumInt(v) => ColumnValue::UMediumInt(v),
            SrcColumnValue::Int(v) => ColumnValue::UInt(v),
            SrcColumnValue::BigInt(v) => ColumnValue::UBigInt(v),
            SrcColumnValue::Float(v) => ColumnValue::Float(v),
            SrcColumnValue::Double(v) => ColumnValue::Double(v),
            SrcColumnValue::Decimal(v) => ColumnValue::Decimal(v),
            SrcColumnValue::String(v) => ColumnValue::VarChar(v),
            SrcColumnValue::Bit(v) => ColumnValue::Bit(v),
            SrcColumnValue::Enum(v) => ColumnValue::Enum(v),
            SrcColumnValue::Set(v) => ColumnValue::Set(v),
            SrcColumnValue::Blob(v) => ColumnValue::Blob(v),
            SrcColumnValue::Year(v) => ColumnValue::Year(v),
            SrcColumnValue::Date(v) => ColumnValue::Date(v),
            SrcColumnValue::Time(v) => ColumnValue::Time(v),
            SrcColumnValue::DateTime(v) => ColumnValue::DateTime(v),
            SrcColumnValue::Timestamp(v) => ColumnValue::Timestamp(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_value_type_name() {
        assert_eq!(ColumnValue::TinyInt(42).type_name(), "TINYINT".to_string());
        assert_eq!(ColumnValue::VarChar("test".to_string()).type_name(), "VARCHAR".to_string());
        assert_eq!(ColumnValue::Null.type_name(), "NULL".to_string());
    }

    #[test]
    fn test_column_value_is_null() {
        assert!(ColumnValue::Null.is_null());
        assert!(!ColumnValue::TinyInt(42).is_null());
    }

    #[test]
    fn test_column_value_size_bytes() {
        assert_eq!(ColumnValue::TinyInt(42).size_bytes(), 1);
        assert_eq!(ColumnValue::BigInt(42).size_bytes(), 8);
        assert_eq!(ColumnValue::VarChar("test".to_string()).size_bytes(), 4);
    }
}
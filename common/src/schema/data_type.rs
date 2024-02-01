use std::hash::{Hash, Hasher};
use bigdecimal::BigDecimal;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::err::CResult;
use crate::err::decode_error::ReError;

#[derive(IntoPrimitive, TryFromPrimitive, Copy, Clone, Debug)]
#[repr(i32)]
pub enum DstColumnType {
    Null = 0,
    Boolean = 1,
    Byte = 2,
    Short = 3,
    Int = 4,
    Long = 5,
    Decimal = 6,
    Double = 7,
    Float = 8,
    Time = 9,
    Date = 10,
    Timestamp = 11,
    Bytes = 12,
    String = 13,
    DateTime = 14,
    // Deprecated
    Other = 19,
    // Deprecated
    MultiValue = 22,
    // Deprecated, use GeoJSON instead
    Geo2D = 23,
    Blob = 24,
    Binary = 25,

    ByteArray = 26,
    ShortArray = 27,
    IntArray = 28,
    FloatArray = 29,
    JSON = 30,
    StringArray = 31,
    DoubleArray = 32,
    LongArray = 33,

    JSONB = 40,
    GeoJSON = 41,
    Geometry = 42,
    Bitmap = 43,
    // Deprecated, not implement
    Map = 50,
}

#[derive(Debug)]
pub enum Value {
    Null,

    Boolean(bool),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),

    String(String),
    JSON(String),

    Float(f32),
    Double(f64),

    //bytes, precision, scale
    Decimal(String),

    Date(i64),
    Time(i64),
    DateTime(i64),
    Timestamp(i64),

    Binary(Vec<u8>),
    Bytes(Vec<u8>),
    Blob(Vec<u8>),

    // ByteArray(Vec<i8>),
    // ShortArray(Vec<i16>),
    // IntArray(Vec<i32>),
    // LongArray(Vec<i64>),
    // FloatArray(Vec<f32>),
    // DoubleArray(Vec<f64>),
    // StringArray(Vec<String>),
}

impl Value {
    pub fn get_data_type(&self) -> DstColumnType {
        match self {
            Value::Null => { DstColumnType::Null}
            Value::Boolean(_) => { DstColumnType::Boolean}
            Value::Byte(_) => { DstColumnType::Byte}
            Value::Short(_) => { DstColumnType::Short}
            Value::Int(_) => { DstColumnType::Int}
            Value::Long(_) => { DstColumnType::Long}
            Value::String(_) => { DstColumnType::String}
            Value::JSON(_) => { DstColumnType::JSON}
            Value::Float(_) => { DstColumnType::Float}
            Value::Double(_) => { DstColumnType::Double}
            Value::Decimal(_) => { DstColumnType::Decimal}
            Value::Date(_) => { DstColumnType::Date}
            Value::Time(_) => { DstColumnType::Time}
            Value::DateTime(_) => { DstColumnType::DateTime}
            Value::Timestamp(_) => { DstColumnType::Timestamp}
            Value::Binary(_) => { DstColumnType::Binary}
            Value::Bytes(_) => { DstColumnType::Bytes}
            Value::Blob(_) => { DstColumnType::Blob}
        }
    }

    pub fn get_data_type_code(&self) -> i32 {
        let data_type = self.get_data_type();
        data_type.into()
    }
}

/// Table Schema
#[derive(Eq, PartialEq, Debug)]
pub struct TableSchema {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

#[derive(Eq, PartialEq)]
pub struct TableSchemaRef<'a> {
    pub catalog: &'a String,
    pub database: &'a String,
    pub table: &'a String,
}

pub struct PrimaryKey(Vec<Value>);


macro_rules! define_from {
    ($($ft: ty, $tt: expr); *) => {
        $(impl From<$ft> for Value {
            #[inline]
            fn from(v: $ft) -> Self {
                $tt(v)
            }
        })*
    };
}

define_from!(
    i64, Value::Long;
    i32, Value::Int;
    i16, Value::Short;
    i8, Value::Byte;
    bool, Value::Boolean;
    f64, Value::Double;
    f32, Value::Float;
    String, Value::String
);

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_owned())
    }
}

impl TryFrom<&str> for TableSchema {
    type Error = ReError;

    #[inline]
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        parse(value)
    }
}

macro_rules! define_parse_data_type_from_str {
    ($($t: expr, $($name: expr),+);*) => {
        fn parse_data_type_from_str(s: &str) -> Result<DstColumnType, ReError> {
            $(
            if $(s.eq_ignore_ascii_case($name) ||)+ false {
                return Ok($t);
            }
            )*
            return Err(ReError::OpMetadataErr(format!("DateType::try_from str: {} err", &s)));
        }
    };
}

define_parse_data_type_from_str!(
    DstColumnType::Null, "null";
    DstColumnType::Boolean, "bool", "boolean";
    DstColumnType::Byte, "byte";
    DstColumnType::Short, "short";
    DstColumnType::Int, "int", "integer";
    DstColumnType::Long, "long", "bigint";
    DstColumnType::Decimal, "decimal";
    DstColumnType::Double, "double";
    DstColumnType::Float, "float";
    DstColumnType::Time, "time";
    DstColumnType::Date, "date";
    DstColumnType::Timestamp, "timestamp";
    DstColumnType::Bytes, "bytes";
    DstColumnType::String, "string", "varchar";
    DstColumnType::DateTime, "datetime";
    DstColumnType::Other, "other";
    DstColumnType::MultiValue, "multiValue";
    DstColumnType::Geo2D, "Geo2D";
    DstColumnType::Blob, "Blob";
    DstColumnType::Binary, "Binary"
);

impl TryFrom<String> for DstColumnType {
    type Error = ReError;
    #[inline]
    fn try_from(s: String) -> Result<Self, Self::Error> {
        parse_data_type_from_str(s.as_ref())
    }
}

impl TableSchema {

    #[inline]
    pub fn create(catalog: &String, schema: &String, table: &String) -> Self {
        TableSchema {
            catalog: catalog.clone(),
            database: schema.clone(),
            table: table.clone(),
        }
    }

}

impl <'a> TableSchemaRef<'a> {

    #[inline]
    pub fn create(catalog: &'a String, schema: &'a String, table: &'a String) -> Self {
        TableSchemaRef {
            catalog,
            database: schema,
            table,
        }
    }

}


/// Support schema string to TableSchema
/// [default.]database_name.table_name
impl TryFrom<String> for TableSchema {
    type Error = ReError;

    #[inline]
    fn try_from(value: String) -> Result<Self, Self::Error> {
        parse(value.as_str())
    }
}

impl Hash for TableSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_str(self.catalog.as_str());
        state.write_str(self.database.as_str());
        state.write_str(self.table.as_str());
    }
}

fn parse(value: &str) -> Result<TableSchema, ReError> {
    let split = value.split(".");
    let keys: Vec<&str> = split.collect();
    if keys.len() !=2 && keys.len() != 3 {
        return Err(ReError::TableSchemaIntoErr(format!(
            "can't From {} into TableSchema", &value
        )));
    }
    let mut i = 0;
    let catalog = if keys.len() == 2 {
        String::from("default")
    } else {
        i += 1;
        check_and_simplify_name(keys[0], value)?
    };
    let database = check_and_simplify_name(keys[i], value)?;
    i += 1;
    let table = check_and_simplify_name(keys[i], value)?;
    Ok(TableSchema {
        catalog, database, table
    })
}

fn check_and_simplify_name(origin_name: &str, value: &str) -> CResult<String> {
    // remove `, eg: `abc` -> abc
    let name = if origin_name.starts_with("`") {
        &origin_name[1..]
    } else {
        origin_name
    };
    let name = if name.ends_with("`") {
        &name[0..(name.len()-1)]
    } else {
        name
    };
    if name.contains("`") {
        return Err(ReError::TableSchemaIntoErr(value.into()));
    }
    Ok(String::from(name))
}

#[cfg(test)]
mod test {
    use crate::err::CResult;
    use crate::schema::data_type::TableSchema;

    #[test]
    fn test_schema() -> CResult<()> {
        let s = "a.b";
        let schema = TableSchema::try_from(s)?;
        assert_eq!("default", schema.catalog);
        assert_eq!("a", schema.database);
        assert_eq!("b", schema.table);
        Ok(())
    }

    #[test]
    fn test_schema1() -> CResult<()> {
        let s = "a.b.c";
        let schema = TableSchema::try_from(s)?;
        assert_eq!("a", schema.catalog);
        assert_eq!("b", schema.database);
        assert_eq!("c", schema.table);
        Ok(())
    }

    #[test]
    fn test_schema2() -> CResult<()> {
        let s = "`a`.`b`.c";
        let schema = TableSchema::try_from(s)?;
        assert_eq!("a", schema.catalog);
        assert_eq!("b", schema.database);
        assert_eq!("c", schema.table);
        Ok(())
    }

    #[test]
    fn test_schema_err0() -> CResult<()> {
        let s = "`a`.`b``.c";
        let schema = TableSchema::try_from(s);
        assert!(schema.is_err());
        Ok(())
    }

    #[test]
    fn test_schema_err1() -> CResult<()> {
        let s = "c";
        let schema = TableSchema::try_from(s);
        assert!(schema.is_err());
        Ok(())
    }

    #[test]
    fn test_schema_err2() -> CResult<()> {
        let s = "a.b.c.d";
        let schema = TableSchema::try_from(s);
        assert!(schema.is_err());
        Ok(())
    }

}

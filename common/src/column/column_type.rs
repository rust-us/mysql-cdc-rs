use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Serialize;

/// MYSQL 数据类型
///
/// type def ref: https://dev.mysql.com/doc/internals/en/table-map-event.html
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum ColumnType {
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,

    Time = 11,
    DateTime = 12,
    Year = 13,
    NewDate = 14, // internal used
    VarChar = 15,
    // /*  u16 --> 2 u8 */ (meta >> 8) as u8, meta as u8
    Bit = 16,
    Timestamp2 = 17, // this field is suck!!! don't know how to parse
    DateTime2 = 18,  // this field is suck!!! don't know how to parse
    Time2 = 19,      // this field is suck!!! don't know how to parse

    Array = 20,
    Invalid = 243,
    Bool = 244,

    /// JSON is MySQL 5.7.8+ type. Not supported in MariaDB.
    Json = 245,
    NewDecimal = 246,
    Enum = 247,       // internal used
    Set = 248,        // internal used
    TinyBlob = 249,   // internal used
    MediumBlob = 250, // internal used
    LongBlob = 251,   // internal used
    Blob = 252,
    VarString = 253,
    String = 254,
    Geometry = 255,
}

impl ColumnType {
    /// return (identifer, bytes used) of column type
    pub fn meta(&self) -> (u16, u8) {
        match *self {
            ColumnType::Decimal => (0, 0),
            ColumnType::Tiny => (1, 0),
            ColumnType::Short => (2, 0),
            ColumnType::Long => (3, 0),
            ColumnType::Float => (4, 1),
            ColumnType::Double => (5, 1),
            ColumnType::Null => (6, 0),
            ColumnType::Timestamp => (7, 0),
            ColumnType::LongLong => (8, 0),
            ColumnType::Int24 => (9, 0),
            ColumnType::Date => (10, 0),
            ColumnType::Time => (11, 0),
            ColumnType::DateTime => (12, 0),
            ColumnType::Year => (13, 0),
            ColumnType::NewDate => (14, 0),
            ColumnType::VarChar => (15, 2),
            ColumnType::Bit => (16, 2),
            ColumnType::Timestamp2 => (17, 1),
            ColumnType::DateTime2 => (18, 1),
            ColumnType::Time2 => (19, 1),

            ColumnType::Array => (20, 0),
            ColumnType::Invalid => (243, 0),
            ColumnType::Bool => (244, 0),

            ColumnType::Json => (245, 2),
            ColumnType::NewDecimal => (246, 2),
            ColumnType::Enum => (247, 0),
            ColumnType::Set => (248, 0),
            ColumnType::TinyBlob => (249, 0),
            ColumnType::MediumBlob => (250, 0),
            ColumnType::LongBlob => (251, 0),
            ColumnType::Blob => (252, 1),
            ColumnType::VarString => (253, 2),
            ColumnType::String => (254, 2),
            ColumnType::Geometry => (255, 1),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::column::column_type::ColumnType;

    #[test]
    fn test() {
        let code = ColumnType::LongBlob;
        assert_eq!(251, u8::from(code));

        let t = ColumnType::try_from(253).unwrap();
        assert_eq!(t, ColumnType::VarString);
    }

    #[test]
    fn test_into() {
        let st: u8 = ColumnType::DateTime.into();
        assert_eq!(st, 12u8);

        let sp = ColumnType::Int24;
        let sp_val:u8 = sp.into();
        assert_eq!(sp_val, 9u8);

        let code = ColumnType::Short;
        let code_val:u8 = code.into();
        assert_eq!(u8::from(code), 2);
        assert_eq!(code_val, 2);
    }

    #[test]
    fn test_try_from() {
        let pk = ColumnType::try_from(11u8);
        assert_eq!(pk, Ok(ColumnType::Time));

        let three = ColumnType::try_from(111u8);
        assert_eq!(
            three.unwrap_err().to_string(),
            "No discriminant in enum `ColumnType` matches the value `111`",
        );
    }
}

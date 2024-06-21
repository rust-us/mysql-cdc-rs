use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Serialize;

/// MYSQL 数据类型
///
/// <table>
///   <caption>Table_map_event column types: numerical identifier and
///   metadata</caption>
///   <tr>
///     <th>Name</th>
///     <th>Identifier</th>
///     <th>Size of metadata in bytes</th>
///     <th>Description of metadata</th>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_DECIMAL</td><td>0</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_TINY</td><td>1</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_SHORT</td><td>2</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_LONG</td><td>3</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_FLOAT</td><td>4</td>
///     <td>1 byte</td>
///     <td>1 byte unsigned integer, representing the "pack_length", which
///     is equal to sizeof(float) on the server from which the event
///     originates.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_DOUBLE</td><td>5</td>
///     <td>1 byte</td>
///     <td>1 byte unsigned integer, representing the "pack_length", which
///     is equal to sizeof(double) on the server from which the event
///     originates.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_NULL</td><td>6</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_TIMESTAMP</td><td>7</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_LONGLONG</td><td>8</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_INT24</td><td>9</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_DATE</td><td>10</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_TIME</td><td>11</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_DATETIME</td><td>12</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_YEAR</td><td>13</td>
///     <td>0</td>
///     <td>No column metadata.</td>
///   </tr>
///
///   <tr>
///     <td><i>MYSQL_TYPE_NEWDATE</i></td><td><i>14</i></td>
///     <td>&ndash;</td>
///     <td><i>This enumeration value is only used internally and cannot
///     exist in a binlog.</i></td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_VARCHAR</td><td>15</td>
///     <td>2 bytes</td>
///     <td>2 byte unsigned integer representing the maximum length of
///     the string.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_BIT</td><td>16</td>
///     <td>2 bytes</td>
///     <td>A 1 byte unsigned int representing the length in bits of the
///     bitfield (0 to 64), followed by a 1 byte unsigned int
///     representing the number of bytes occupied by the bitfield.  The
///     number of bytes is either int((length + 7) / 8) or int(length / 8).
///     </td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_NEWDECIMAL</td><td>246</td>
///     <td>2 bytes</td>
///     <td>A 1 byte unsigned int representing the precision, followed
///     by a 1 byte unsigned int representing the number of decimals.</td>
///   </tr>
///
///   <tr>
///     <td><i>MYSQL_TYPE_ENUM</i></td><td><i>247</i></td>
///     <td>&ndash;</td>
///     <td><i>This enumeration value is only used internally and cannot
///     exist in a binlog.</i></td>
///   </tr>
///
///   <tr>
///     <td><i>MYSQL_TYPE_SET</i></td><td><i>248</i></td>
///     <td>&ndash;</td>
///     <td><i>This enumeration value is only used internally and cannot
///     exist in a binlog.</i></td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_TINY_BLOB</td><td>249</td>
///     <td>&ndash;</td>
///     <td><i>This enumeration value is only used internally and cannot
///     exist in a binlog.</i></td>
///   </tr>
///
///   <tr>
///     <td><i>MYSQL_TYPE_MEDIUM_BLOB</i></td><td><i>250</i></td>
///     <td>&ndash;</td>
///     <td><i>This enumeration value is only used internally and cannot
///     exist in a binlog.</i></td>
///   </tr>
///
///   <tr>
///     <td><i>MYSQL_TYPE_LONG_BLOB</i></td><td><i>251</i></td>
///     <td>&ndash;</td>
///     <td><i>This enumeration value is only used internally and cannot
///     exist in a binlog.</i></td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_BLOB</td><td>252</td>
///     <td>1 byte</td>
///     <td>The pack length, i.e., the number of bytes needed to represent
///     the length of the blob: 1, 2, 3, or 4.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_VAR_STRING</td><td>253</td>
///     <td>2 bytes</td>
///     <td>This is used to store both strings and enumeration values.
///     The first byte is a enumeration value storing the <i>real
///     type</i>, which may be either MYSQL_TYPE_VAR_STRING or
///     MYSQL_TYPE_ENUM.  The second byte is a 1 byte unsigned integer
///     representing the field size, i.e., the number of bytes needed to
///     store the length of the string.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_STRING</td><td>254</td>
///     <td>2 bytes</td>
///     <td>The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253).
///     The second byte is the field size, i.e., the number of bytes in
///     the representation of size of the string: 3 or 4.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_GEOMETRY</td><td>255</td>
///     <td>1 byte</td>
///     <td>The pack length, i.e., the number of bytes needed to represent
///     the length of the geometry: 1, 2, 3, or 4.</td>
///   </tr>
///
///   <tr>
///     <td>MYSQL_TYPE_TYPED_ARRAY</td><td>15</td>
///     <td>up to 4 bytes</td>
///     <td>
///       - The first byte holds the MySQL type for the elements.
///       - The following 0, 1, 2, or 3 bytes holds the metadata for the MySQL
///         type for the elements. The contents of these bytes depends on the
///         element type, as described in the other rows of this table.
///     </td>
///   </tr>
///
///   </table>
///
/// type def ref: https://dev.mysql.com/doc/dev/mysql-server/latest/rows__event_8h_source.html
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum SrcColumnType {
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

impl SrcColumnType {
    /// return (identifer, bytes used) of column type
    pub fn meta(&self) -> (u16, u8) {
        match *self {
            SrcColumnType::Decimal => (0, 0),
            SrcColumnType::Tiny => (1, 0),
            SrcColumnType::Short => (2, 0),
            SrcColumnType::Long => (3, 0),
            SrcColumnType::Float => (4, 1),
            SrcColumnType::Double => (5, 1),
            SrcColumnType::Null => (6, 0),
            SrcColumnType::Timestamp => (7, 0),
            SrcColumnType::LongLong => (8, 0),
            SrcColumnType::Int24 => (9, 0),
            SrcColumnType::Date => (10, 0),
            SrcColumnType::Time => (11, 0),
            SrcColumnType::DateTime => (12, 0),
            SrcColumnType::Year => (13, 0),
            SrcColumnType::NewDate => (14, 0),
            SrcColumnType::VarChar => (15, 2),
            SrcColumnType::Bit => (16, 2),
            SrcColumnType::Timestamp2 => (17, 1),
            SrcColumnType::DateTime2 => (18, 1),
            SrcColumnType::Time2 => (19, 1),

            SrcColumnType::Array => (20, 0),
            SrcColumnType::Invalid => (243, 0),
            SrcColumnType::Bool => (244, 0),

            SrcColumnType::Json => (245, 2),
            SrcColumnType::NewDecimal => (246, 2),
            SrcColumnType::Enum => (247, 0),
            SrcColumnType::Set => (248, 0),
            SrcColumnType::TinyBlob => (249, 0),
            SrcColumnType::MediumBlob => (250, 0),
            SrcColumnType::LongBlob => (251, 0),
            SrcColumnType::Blob => (252, 1),
            SrcColumnType::VarString => (253, 2),
            SrcColumnType::String => (254, 2),
            SrcColumnType::Geometry => (255, 1),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::binlog::column::column_type::SrcColumnType;

    #[test]
    fn test() {
        let code = SrcColumnType::LongBlob;
        assert_eq!(251, u8::from(code));

        let t = SrcColumnType::try_from(253).unwrap();
        assert_eq!(t, SrcColumnType::VarString);
    }

    #[test]
    fn test_into() {
        let st: u8 = SrcColumnType::DateTime.into();
        assert_eq!(st, 12u8);

        let sp = SrcColumnType::Int24;
        let sp_val:u8 = sp.into();
        assert_eq!(sp_val, 9u8);

        let code = SrcColumnType::Short;
        let code_val:u8 = code.into();
        assert_eq!(u8::from(code), 2);
        assert_eq!(code_val, 2);
    }

    // #[test]
    // fn test_try_from() {
    //     let pk = SrcColumnType::try_from(11u8);
    //     assert_eq!(pk, Ok(SrcColumnType::Time));
    //
    //     let three = SrcColumnType::try_from(111u8);
    //     assert_eq!(
    //         three.unwrap_err().to_string(),
    //         "No discriminant in enum `ColumnType` matches the value `111`",
    //     );
    // }
}

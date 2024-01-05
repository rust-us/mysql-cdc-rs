use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Serialize;
use crate::column::column_value::{ColumnValues, parse_packed};
use nom::{
    bytes::complete::take,
    combinator::map,
    number::complete::{le_u16, le_u8},
    IResult,
};
use crate::row::decimal::decimal_length;
use crate::utils::pu32;

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

    pub fn parse_cell<'a>(&self, input: &'a [u8], meta:u16) -> IResult<&'a [u8], (usize, ColumnValues)> {
        match *self {
            ColumnType::Decimal => {
                map(take(4usize), |s: &[u8]| (4, ColumnValues::Decimal(s.to_vec())))(input)
            }
            ColumnType::Tiny => map(take(1usize), |s: &[u8]| (1, ColumnValues::Tiny(s.to_vec())))(input),
            ColumnType::Short => {
                map(take(2usize), |s: &[u8]| (2, ColumnValues::Short(s.to_vec())))(input)
            }
            ColumnType::Long => map(take(4usize), |s: &[u8]| (4, ColumnValues::Long(s.to_vec())))(input),
            ColumnType::Float => map(take(4usize), |s: &[u8]| {
                let mut f: [u8; 4] = Default::default();
                f.copy_from_slice(s);
                (4, ColumnValues::Float(f32::from_le_bytes(f)))
            })(input),
            ColumnType::Double => map(take(8usize), |s: &[u8]| {
                let mut d: [u8; 8] = Default::default();
                d.copy_from_slice(s);
                (8, ColumnValues::Double(f64::from_le_bytes(d)))
            })(input),
            ColumnType::Null => map(take(0usize), |_| (0, ColumnValues::Null))(input),
            ColumnType::LongLong => map(take(8usize), |s: &[u8]| {
                (8, ColumnValues::LongLong(s.to_vec()))
            })(input),
            ColumnType::Int24 => {
                map(take(4usize), |s: &[u8]| (4, ColumnValues::Int24(s.to_vec())))(input)
            }
            ColumnType::Timestamp => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::Timestamp(v))
            })(input),
            ColumnType::Date => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::Date(v))
            })(input),
            ColumnType::Time => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::Time(v))
            })(input),
            ColumnType::DateTime => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::DateTime(v))
            })(input),
            ColumnType::Year => map(take(2usize), |s: &[u8]| (2, ColumnValues::Year(s.to_vec())))(input),
            ColumnType::NewDate => map(take(0usize), |_| (0, ColumnValues::NewDate))(input),
            // ref: https://dev.mysql.com/doc/refman/5.7/en/char.html
            ColumnType::VarChar => {
                if meta > 255 {
                    let (i, len) = le_u16(input)?;
                    map(take(len), move |s: &[u8]| {
                        (len as usize + 2, ColumnValues::VarChar(s.to_vec()))
                    })(i)
                } else {
                    let (i, len) = le_u8(input)?;
                    map(take(len), move |s: &[u8]| {
                        (len as usize + 1, ColumnValues::VarChar(s.to_vec()))
                    })(i)
                }
            }
            ColumnType::Bit => unreachable!(),
            // ColumnType::Bit(b1, b2) => {
            //     let len = ((b1 + 7) / 8 + (b2 + 7) / 8) as usize;
            //     map(take(len), move |s: &[u8]| (len, ColumnValues::Bit(s.to_vec())))(input)
            // }
            ColumnType::Timestamp2 => map(take(4usize), |v: &[u8]| {
                (4, ColumnValues::Timestamp2(v.to_vec()))
            })(input),
            ColumnType::DateTime2 => map(take(4usize), |v: &[u8]| {
                (4, ColumnValues::DateTime2(v.to_vec()))
            })(input),
            ColumnType::Time2 => {
                map(take(4usize), |v: &[u8]| (4, ColumnValues::Time2(v.to_vec())))(input)
            },
            ColumnType::Json => unreachable!(),
            // ColumnTypes::NewDecimal(precision, scale) => {
            ColumnType::NewDecimal => {
                let (length, _, _, _, _, _, _) = decimal_length(meta);

                map(take(length), move |s: &[u8]| {
                    (length as usize, ColumnValues::NewDecimal(s.to_vec()))
                })(input)
            }
            ColumnType::Enum => map(take(0usize), |_| (0, ColumnValues::Enum))(input),
            ColumnType::Set => map(take(0usize), |_| (0, ColumnValues::Set))(input),
            ColumnType::TinyBlob => map(take(0usize), |_| (0, ColumnValues::TinyBlob))(input),
            ColumnType::MediumBlob => map(take(0usize), |_| (0, ColumnValues::MediumBlob))(input),
            ColumnType::LongBlob => map(take(0usize), |_| (0, ColumnValues::LongBlob))(input),
            ColumnType::Blob => {
                let len_bytes = meta;

                let mut raw_len = input[..len_bytes as usize].to_vec();
                for _ in 0..(4 - len_bytes) {
                    raw_len.push(0);
                }
                let (_, len) = pu32(&raw_len).unwrap();
                map(take(len), move |s: &[u8]| {
                    (
                        len_bytes as usize + len as usize,
                        ColumnValues::Blob(s.to_vec()),
                    )
                })(&input[len_bytes as usize..])
            }
            ColumnType::VarString => {
                // TODO should check string max_len ?
                let (i, len) = le_u8(input)?;
                map(take(len), move |s: &[u8]| {
                    (len as usize, ColumnValues::VarString(s.to_vec()))
                })(i)
            }
            ColumnType::String => {
                // TODO should check string max_len ?
                let (i, len) = le_u8(input)?;
                map(take(len), move |s: &[u8]| {
                    (len as usize, ColumnValues::VarChar(s.to_vec()))
                })(i)
            }
            // // TODO fix do not use len in def ?
            ColumnType::Geometry=> {
                let len_ = meta;

                let x = map(take(len_), |s: &[u8]| {
                    (len_ as usize, ColumnValues::Geometry(s.to_vec()))
                })(input);

                x
            },
            // 20 => ColumnTypes::Array,
            // 243 => ColumnTypes::Invalid,
            // 244 => ColumnTypes::Bool,
            ColumnType::Array | ColumnType::Invalid | ColumnType::Bool => {
                unreachable!()
            },
        }
    }

    // /// Decode field metadata by column types.
    // ///
    // /// @see mysql-5.1.60/sql/rpl_utility.h
    // pub fn decode_fields_def<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], (usize, Self)> {
    //     match *self {
    //         // internal used
    //         // ColumnTypes::TinyBlob、ColumnTypes::MediumBlob、ColumnTypes::LongBlob
    //         ColumnTypes::Blob(_) => map(le_u8, |v| (1, ColumnTypes::Blob(v)))(input),
    //         ColumnTypes::Double(_) => map(le_u8, |v| (1, ColumnTypes::Double(v)))(input),
    //         ColumnTypes::Float(_) => map(le_u8, |v| (1, ColumnTypes::Float(v)))(input),
    //         ColumnTypes::Geometry(_) => map(le_u8, |v| (1, ColumnTypes::Geometry(v)))(input),
    //         ColumnTypes::Time2(_) => map(le_u8, |v| (1, ColumnTypes::Timestamp2(v)))(input),
    //         ColumnTypes::DateTime2(_) => map(le_u8, |v| (1, ColumnTypes::DateTime2(v)))(input),
    //         ColumnTypes::Timestamp2(_) => map(le_u8, |v| (1, ColumnTypes::Timestamp2(v)))(input),
    //         ColumnTypes::Json(_) => map(le_u8, |v| (1, ColumnTypes::Json(v)))(input),
    //
    //         ColumnTypes::Bit(_, _) => {
    //             map(tuple((le_u8, le_u8)), |(b1, b2)| (2, ColumnTypes::Bit(b1, b2)))(input)
    //         }
    //         ColumnTypes::VarChar(_) => map(le_u16, |v| (2, ColumnTypes::VarChar(v)))(input),
    //         ColumnTypes::NewDecimal(_, _) => map(tuple((le_u8, le_u8)), |(m, d)| {
    //             (2, ColumnTypes::NewDecimal(m, d))
    //         })(input),
    //
    //         ColumnTypes::VarString(_, _) => map(tuple((le_u8, le_u8)), |(t, len)| {
    //             (2, ColumnTypes::VarString(t, len))
    //         })(input),
    //         ColumnTypes::String(_, _) => map(tuple((le_u8, le_u8)), |(t, len)| {
    //             (2, ColumnTypes::String(t, len))
    //         })(input),
    //         _ => Ok((input, (0, self.clone()))),
    //     }
    // }
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

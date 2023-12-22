use crate::utils::pu32;
use nom::{
    bytes::complete::take,
    combinator::map,
    number::complete::{le_u16, le_u8},
    sequence::tuple,
    IResult,
};
use serde::Serialize;
use crate::column::column_value::parse_packed;
use crate::ColumnValues;

/// MYSQL 数据类型
///
/// type def ref: https://dev.mysql.com/doc/internals/en/table-map-event.html
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum ColumnTypes {
    Decimal,
    Tiny,
    Short,
    Long,
    Float(u8),
    Double(u8),
    Null,
    Timestamp,
    LongLong,
    Int24,
    Date,

    Time,
    DateTime,
    Year,
    NewDate, // internal used
    VarChar(u16),
    Bit(u8, u8),
    Timestamp2(u8), // this field is suck!!! don't know how to parse
    DateTime2(u8),  // this field is suck!!! don't know how to parse
    Time2(u8),      // this field is suck!!! don't know how to parse

    Array,
    Invalid,
    Bool,

    /// JSON is MySQL 5.7.8+ type. Not supported in MariaDB.
    Json(u8),
    NewDecimal(u8, u8),
    Enum,       // internal used
    Set,        // internal used
    TinyBlob,   // internal used
    MediumBlob, // internal used
    LongBlob,   // internal used
    Blob(u8),
    VarString(u8, u8),
    String(u8, u8),
    Geometry(u8),
}


impl ColumnTypes {
//     /// del
//     /// return (identifer, bytes used) of column type
//     pub fn meta(&self) -> (u8, u8) {
//         match *self {
//             ColumnTypes::Decimal => (0, 0),
//             ColumnTypes::Tiny => (1, 0),
//             ColumnTypes::Short => (2, 0),
//             ColumnTypes::Long => (3, 0),
//             ColumnTypes::Float(_) => (4, 1),
//             ColumnTypes::Double(_) => (5, 1),
//             ColumnTypes::Null => (6, 0),
//             ColumnTypes::Timestamp => (7, 0),
//             ColumnTypes::LongLong => (8, 0),
//             ColumnTypes::Int24 => (9, 0),
//             ColumnTypes::Date => (10, 0),
//             ColumnTypes::Time => (11, 0),
//             ColumnTypes::DateTime => (12, 0),
//             ColumnTypes::Year => (13, 0),
//             ColumnTypes::NewDate => (14, 0),
//             ColumnTypes::VarChar(_) => (15, 2),
//             ColumnTypes::Bit(a, b) => (16, 2),
//             ColumnTypes::Timestamp2(_) => (17, 1),
//             ColumnTypes::DateTime2(_) => (18, 1),
//             ColumnTypes::Time2(_) => (19, 1),
//             ColumnTypes::Array => (20, 0),
//             ColumnTypes::Invalid => (243, 0),
//             ColumnTypes::Bool => (244, 0),
//             ColumnTypes::Json(_) => (245, 2),
//             ColumnTypes::NewDecimal(_, _) => (246, 2),
//             ColumnTypes::Enum => (247, 0),
//             ColumnTypes::Set => (248, 0),
//             ColumnTypes::TinyBlob => (249, 0),
//             ColumnTypes::MediumBlob => (250, 0),
//             ColumnTypes::LongBlob => (251, 0),
//             ColumnTypes::Blob(_) => (252, 1),
//             ColumnTypes::VarString(_, _) => (253, 2),
//             ColumnTypes::String(_, _) => (254, 2),
//             ColumnTypes::Geometry(_) => (255, 1),
//         }
//     }

    pub fn parse_cell<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], (usize, ColumnValues)> {
        match *self {
            ColumnTypes::Decimal => {
                map(take(4usize), |s: &[u8]| (4, ColumnValues::Decimal(s.to_vec())))(input)
            }
            ColumnTypes::Tiny => map(take(1usize), |s: &[u8]| (1, ColumnValues::Tiny(s.to_vec())))(input),
            ColumnTypes::Short => {
                map(take(2usize), |s: &[u8]| (2, ColumnValues::Short(s.to_vec())))(input)
            }
            ColumnTypes::Long => map(take(4usize), |s: &[u8]| (4, ColumnValues::Long(s.to_vec())))(input),
            ColumnTypes::Float(_) => map(take(4usize), |s: &[u8]| {
                let mut f: [u8; 4] = Default::default();
                f.copy_from_slice(s);
                (4, ColumnValues::Float(f32::from_le_bytes(f)))
            })(input),
            ColumnTypes::Double(_) => map(take(8usize), |s: &[u8]| {
                let mut d: [u8; 8] = Default::default();
                d.copy_from_slice(s);
                (8, ColumnValues::Double(f64::from_le_bytes(d)))
            })(input),
            ColumnTypes::Null => map(take(0usize), |_| (0, ColumnValues::Null))(input),
            ColumnTypes::LongLong => map(take(8usize), |s: &[u8]| {
                (8, ColumnValues::LongLong(s.to_vec()))
            })(input),
            ColumnTypes::Int24 => {
                map(take(4usize), |s: &[u8]| (4, ColumnValues::Int24(s.to_vec())))(input)
            }
            ColumnTypes::Timestamp => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::Timestamp(v))
            })(input),
            ColumnTypes::Date => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::Date(v))
            })(input),
            ColumnTypes::Time => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::Time(v))
            })(input),
            ColumnTypes::DateTime => map(parse_packed, |(len, v): (usize, Vec<u8>)| {
                (len, ColumnValues::DateTime(v))
            })(input),
            ColumnTypes::Year => map(take(2usize), |s: &[u8]| (2, ColumnValues::Year(s.to_vec())))(input),
            ColumnTypes::NewDate => map(take(0usize), |_| (0, ColumnValues::NewDate))(input),
            // ref: https://dev.mysql.com/doc/refman/5.7/en/char.html
            ColumnTypes::VarChar(max_len) => {
                if max_len > 255 {
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
            ColumnTypes::Bit(b1, b2) => {
                let len = ((b1 + 7) / 8 + (b2 + 7) / 8) as usize;
                map(take(len), move |s: &[u8]| (len, ColumnValues::Bit(s.to_vec())))(input)
            }
            ColumnTypes::Timestamp2(_) => map(take(4usize), |v: &[u8]| {
                (4, ColumnValues::Timestamp2(v.to_vec()))
            })(input),
            ColumnTypes::DateTime2(_) => map(take(4usize), |v: &[u8]| {
                (4, ColumnValues::DateTime2(v.to_vec()))
            })(input),
            ColumnTypes::Time2(_) => {
                map(take(4usize), |v: &[u8]| (4, ColumnValues::Time2(v.to_vec())))(input)
            },
            ColumnTypes::Json(_) => todo!(),
            ColumnTypes::NewDecimal(precision, scale) => {
                // copy from https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/src/binary_log_funcs.cpp#L204-L214
                let dig2bytes: [u8; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
                let intg = precision - scale;
                let intg0 = intg / 9;
                let frac0 = scale / 9;
                let intg0x = intg - intg0 * 9;
                let frac0x = scale - frac0 * 9;
                let len =
                    intg0 * 4 + dig2bytes[intg0x as usize] + frac0 * 4 + dig2bytes[frac0x as usize];
                map(take(len), move |s: &[u8]| {
                    (len as usize, ColumnValues::NewDecimal(s.to_vec()))
                })(input)
            }
            ColumnTypes::Enum => map(take(0usize), |_| (0, ColumnValues::Enum))(input),
            ColumnTypes::Set => map(take(0usize), |_| (0, ColumnValues::Set))(input),
            ColumnTypes::TinyBlob => map(take(0usize), |_| (0, ColumnValues::TinyBlob))(input),
            ColumnTypes::MediumBlob => map(take(0usize), |_| (0, ColumnValues::MediumBlob))(input),
            ColumnTypes::LongBlob => map(take(0usize), |_| (0, ColumnValues::LongBlob))(input),
            ColumnTypes::Blob(len_bytes) => {
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
            ColumnTypes::VarString(_, _) => {
                // TODO should check string max_len ?
                let (i, len) = le_u8(input)?;
                map(take(len), move |s: &[u8]| {
                    (len as usize, ColumnValues::VarString(s.to_vec()))
                })(i)
            }
            ColumnTypes::String(_, _) => {
                // TODO should check string max_len ?
                let (i, len) = le_u8(input)?;
                map(take(len), move |s: &[u8]| {
                    (len as usize, ColumnValues::VarChar(s.to_vec()))
                })(i)
            }
            // TODO fix do not use len in def ?
            ColumnTypes::Geometry(len) => map(take(len), |s: &[u8]| {
                (len as usize, ColumnValues::Geometry(s.to_vec()))
            })(input),
            // 20 => ColumnTypes::Array,
            // 243 => ColumnTypes::Invalid,
            // 244 => ColumnTypes::Bool,
            ColumnTypes::Array | ColumnTypes::Invalid | ColumnTypes::Bool => {
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

impl From<u8> for ColumnTypes {
    /// enum_field_types
    fn from(code: u8) -> Self {
        let value = match code {
            0 => ColumnTypes::Decimal,
            1 => ColumnTypes::Tiny,
            2 => ColumnTypes::Short,
            3 => ColumnTypes::Long,
            4 => ColumnTypes::Float(4),
            5 => ColumnTypes::Double(8),
            6 => ColumnTypes::Null,
            7 => ColumnTypes::Timestamp,
            8 => ColumnTypes::LongLong,
            9 => ColumnTypes::Int24,
            10 => ColumnTypes::Date,
            11 => ColumnTypes::Time,
            12 => ColumnTypes::DateTime,
            13 => ColumnTypes::Year,
            14 => ColumnTypes::NewDate,
            15 => ColumnTypes::VarChar(0),
            16 => ColumnTypes::Bit(0, 0),
            17 => ColumnTypes::Timestamp2(0),
            18 => ColumnTypes::DateTime2(0),
            19 => ColumnTypes::Time2(0),
            20 => ColumnTypes::Array,
            243 => ColumnTypes::Invalid,
            244 => ColumnTypes::Bool,
            245 => ColumnTypes::Json(0),
            246 => ColumnTypes::NewDecimal(10, 0),
            247 => ColumnTypes::Enum,
            248 => ColumnTypes::Set,
            249 => ColumnTypes::TinyBlob,
            250 => ColumnTypes::MediumBlob,
            251 => ColumnTypes::LongBlob,
            252 => ColumnTypes::Blob(1),
            253 => ColumnTypes::VarString(1, 0),
            254 => ColumnTypes::String(253, 0),
            255 => ColumnTypes::Geometry(1),
            _ => {
                unreachable!();
                // return Err(DecodeError::String(format!("Unknown column type {}", code)))
            }
        };

        // Ok(value)
        value
    }
}
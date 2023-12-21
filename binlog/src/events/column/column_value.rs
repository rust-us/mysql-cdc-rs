use std::error::Error;
use crate::utils::pu32;
use nom::{
    bytes::complete::take,
    combinator::map,
    number::complete::{le_u16, le_u8},
    IResult,
};
use serde::Serialize;


#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum ColumnValues {
    Decimal(Vec<u8>),
    Tiny(Vec<u8>),
    Short(Vec<u8>),
    Long(Vec<u8>),
    Float(f32),
    Double(f64),
    Null,
    Timestamp(Vec<u8>),
    LongLong(Vec<u8>),
    Int24(Vec<u8>),
    Date(Vec<u8>),
    Time(Vec<u8>),
    DateTime(Vec<u8>),
    Year(Vec<u8>),
    NewDate, // internal used
    VarChar(Vec<u8>),
    Bit(Vec<u8>),
    Timestamp2(Vec<u8>),
    DateTime2(Vec<u8>),
    Time2(Vec<u8>),
    Json(Vec<u8>),
    NewDecimal(Vec<u8>),
    Enum,       // internal used
    Set,        // internal used
    TinyBlob,   // internal used
    MediumBlob, // internal used
    LongBlob,   // internal used
    Blob(Vec<u8>),
    VarString(Vec<u8>),
    String(Vec<u8>),
    Geometry(Vec<u8>),
}


pub fn parse_packed(input: &[u8]) -> IResult<&[u8], (usize, Vec<u8>)> {
    let mut data = vec![input[0]];
    let (i, len) = le_u8(input)?;
    let (i, raw) = take(len)(i)?;
    data.extend(raw);
    Ok((i, (len as usize + 1, data)))
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
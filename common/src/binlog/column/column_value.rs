use std::error::Error;
use serde::Serialize;

/// Type	Storage (Bytes)	Minimum Value Signed	Minimum Value Unsigned	Maximum Value Signed	Maximum Value Unsigned
/// TINYINT	1	-128	0	127	255
/// SMALLINT	2	-32768	0	32767	65535
/// MEDIUMINT	3	-8388608	0	8388607	16777215
/// INT	4	-2147483648	0	2147483647	4294967295
/// BIGINT	8	-263	0	263-1	264-1
#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum SrcColumnValue {
    TinyInt(u8),
    SmallInt(u16),
    MediumInt(u32),
    Int(u32),
    BigInt(u64),

    Float(f32),
    Double(f64),
    Decimal(String),
    String(String),
    Bit(Vec<bool>),
    Enum(u32),
    Set(u64),
    Blob(Vec<u8>),
    Year(u16),
    Date(Date),
    Time(Time),
    DateTime(DateTime),
    Timestamp(u64), // millis from unix time
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct Date {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct Time {
    pub hour: i16, // Signed value from -838 to 838
    pub minute: u8,
    pub second: u8,
    pub millis: u32,
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct DateTime {
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub millis: u32,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
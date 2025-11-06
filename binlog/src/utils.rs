#![allow(dead_code)]

use std::error::Error;
use std::io;
use std::io::{BufRead, Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use nom::{
    bytes::complete::{take, take_till},
    combinator::map,
    number::complete::{le_u16, le_u32, le_u64, le_u8},
    IResult,
};
use common::err::decode_error::ReError;
use crate::NULL_TERMINATOR;


/// extract n(n <= len(input)) bytes string
/// 实现思路：
/// 由于可能存在多个终止符，首先需要找到第一个终止符位置，然后使用 String::from_utf8_lossy 将之前的字符转换为字符串。
pub fn extract_string(input: &[u8]) -> String {
    let null_end = input
        .iter()
        .position(|&c| c == b'\0')
        .unwrap_or(input.len());
    String::from_utf8_lossy(&input[0..null_end]).to_string()
}

/// parse fixed len int
///
/// ref: https://dev.mysql.com/doc/internals/en/integer.html#fixed-length-integer
pub fn int_fixed<'a>(input: &'a [u8], len: u8) -> IResult<&'a [u8], u64> {
    match len {
        1 => map(le_u8, |v| v as u64)(input),
        2 => map(le_u16, |v| v as u64)(input),
        3 | 6 => map(take(3usize), |s: &[u8]| {
            let mut filled = s.to_vec();
            if len == 3 {
                filled.extend(vec![0, 0, 0, 0, 0]);
            } else {
                filled.extend(vec![0, 0]);
            }
            pu64(&filled).unwrap().1
        })(input),
        4 => map(le_u32, |v| v as u64)(input),
        8 => le_u64(input),
        _ => unreachable!(),
    }
}

/// parse len encoded int, is PackedLong, return (used_bytes, value).
///
/// if first byte is less than 0xFB - Integer value is this 1 byte integer
/// 0xFB - NULL value
/// 0xFC - Integer value is encoded in the next 2 bytes (3 bytes total)
/// 0xFD - Integer value is encoded in the next 3 bytes (4 bytes total)
/// 0xFE - Integer value is encoded in the next 8 bytes (9 bytes total)
///
/// ref: https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
pub fn read_len_enc_num_with_slice(slice: &[u8]) -> Result<(usize, u64), ReError> {
    let mut cursor = Cursor::new(slice);

    read_len_enc_num(&mut cursor)
}

/// parse len encoded int, is PackedLong, return (used_bytes, value).
///
/// if first byte is less than 0xFB - Integer value is this 1 byte integer
/// 0xFB - NULL value
/// 0xFC - Integer value is encoded in the next 2 bytes (3 bytes total)
/// 0xFD - Integer value is encoded in the next 3 bytes (4 bytes total)
/// 0xFE - Integer value is encoded in the next 8 bytes (9 bytes total)
///
/// ref: https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
pub fn read_len_enc_num(cursor: &mut Cursor<&[u8]>) -> Result<(usize, u64), ReError> {
    let first_byte = cursor.read_u8()?;

    // 0 -- 250
    if first_byte < 0xFB {
        Ok((1, first_byte as u64))
    } else if first_byte == 0xFB {  // 251
        Err(ReError::String(
            "Length encoded integer cannot be NULL.".to_string(),
        ))
    } else if first_byte == 0xFC { // 252
        Ok((3, cursor.read_u16::<LittleEndian>()? as u64))
    } else if first_byte == 0xFD { // 253
        Ok((4, cursor.read_u24::<LittleEndian>()? as u64))
    } else if first_byte == 0xFE { // 254
        Ok((9, cursor.read_u64::<LittleEndian>()? as u64))
    } else {
        let value = format!("Unexpected length-encoded integer: {}", first_byte).to_string();
        Err(ReError::String(value))
    }
}

pub fn read_string(cursor: &mut Cursor<&[u8]>, size: usize) -> Result<String, ReError> {
    let mut vec = vec![0; size];
    cursor.read_exact(&mut vec)?;

    let str = String::from_utf8_lossy(&vec.clone()).to_string();
    // let str = String::from_utf8(vec.clone())?;
    // let str2 = String::from_utf8_lossy(&vec.clone()).to_string();
    // let str2 = String::from_utf8(vec.clone())?;

    Ok(str)
}

/// 读取变长string，允许null值出现
///
/// ref: https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
pub fn read_len_enc_str_with_cursor_allow_null(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, ReError> {
    let first_byte = cursor.read_u8()?;

    let mut length = 0u64;
    // 0 -- 250
    if first_byte < 0xFB {
        length = first_byte as u64;
    } else if first_byte == 0xFB {  // 251
        return Ok(None);
    } else if first_byte == 0xFC { // 252
        length = cursor.read_u16::<LittleEndian>()? as u64
    } else if first_byte == 0xFD { // 253
        length = cursor.read_u24::<LittleEndian>()? as u64
    } else if first_byte == 0xFE { // 254
        length = cursor.read_u64::<LittleEndian>()?
    } else {
        let value = format!("Unexpected length-encoded integer: {}", first_byte).to_string();
        return Err(ReError::String(value));
    }
    Ok(Some(read_string(cursor, length as usize)?))
}

/// parse length encoded string
///
/// ref: https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
pub fn read_len_enc_str_with_cursor(cursor: &mut Cursor<&[u8]>) -> Result<String, ReError> {
    let (_, length) = read_len_enc_num(cursor)?;

    Ok(read_string(cursor, length as usize)?)
}

pub fn read_null_term_string_with_cursor(cursor: &mut Cursor<&[u8]>) -> Result<String, ReError> {
    let mut vec = Vec::new();
    cursor.read_until(NULL_TERMINATOR, &mut vec)?;
    vec.pop();
    Ok(String::from_utf8(vec)?)
}

/// parse 'null terminated string', consume null byte
///
/// ref: https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString
pub fn read_null_term_string(input: &[u8]) -> IResult<&[u8], String> {
    let (i, ret) = map(take_till(|c: u8| c == 0x00), |s| {
        String::from_utf8_lossy(s).to_string()
    })(input)?;
    let (i, _) = take(1usize)(i)?;
    Ok((i, ret))
}

/// extract len bytes string
///
/// ref: https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::VariableLengthString
pub fn read_variable_len_string(input: &[u8], len: usize) -> String {
    if input.len() <= len {
        String::from_utf8_lossy(&input).to_string()
    } else {
        String::from_utf8_lossy(&input[0..len]).to_string()
    }
}

/// 定长编码取值, parse fixed len string。
/// 第一个byte申明长度len，后续len个byte为存储的值
///
/// ref: https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString
pub fn read_fixed_len_string_with_cursor(cursor: &mut Cursor<&[u8]>) -> Result<String, ReError> {
    let len = cursor.read_u8()?;

    read_string(cursor, len as usize)
}

/// Reads bitmap in little-endian bytes order
pub fn read_bitmap_little_endian_bits(cursor: &mut Cursor<&[u8]>, bits_number: usize)
                                 -> Result<Vec<u8>, io::Error> {
    let mut result = vec![0; bits_number];

    let bytes_number = (bits_number + 7) / 8;
    for bit in 0..bytes_number {
        let flag = cursor.read_u8()?;
        //  fixed
        let _flag = flag & 0xff;
        if _flag == 0 {
            continue;
        }

        for y in 0..8 {
            let index = (bit << 3) + y;
            if index == bits_number {
                break;
            }
            result[index] = (flag & (1 << y));
        }
    }

    Ok(result)
}

pub fn read_bitmap_little_endian(cursor: &mut Cursor<&[u8]>, bits_number: usize) -> Result<Vec<bool>, io::Error> {
    let mut result = vec![false; bits_number];

    let bytes_number = (bits_number + 7) / 8;
    for bit in 0..bytes_number {
        let flag = cursor.read_u8()?;
        //  fixed
        let _flag = flag & 0xff;
        if _flag == 0 {
            continue;
        }

        for y in 0..8 {
            let index = (bit << 3) + y;
            if index == bits_number {
                break;
            }
            result[index] = (flag & (1 << y)) > 0;
        }
    }
    Ok(result)
}

/// Reads bitmap in big-endian bytes order
pub fn read_bitmap_big_endian(
    cursor: &mut Cursor<&[u8]>,
    bits_number: usize,
) -> Result<Vec<bool>, io::Error> {
    let mut result = vec![false; bits_number];
    let bytes_number = (bits_number + 7) / 8;
    for i in 0..bytes_number {
        let value = cursor.read_u8()?;
        for y in 0..8 {
            let index = ((bytes_number - i - 1) << 3) + y;
            if index >= bits_number {
                continue;
            }
            result[index] = (value & (1 << y)) > 0;
        }
    }
    Ok(result)
}

//////////////////////////////////////////////// Write
pub fn pu32(input: &[u8]) -> IResult<&[u8], u32> {
    le_u32(input)
}

pub fn pu64(input: &[u8]) -> IResult<&[u8], u64> {
    le_u64(input)
}

//////////////////////////////////////////////// Legacy parsing functions for backward compatibility

use byteorder::BigEndian;
use common::binlog::column::column_value::{Date, DateTime, Time};

// Helper functions for reading extended integer types
fn read_u24_le(cursor: &mut Cursor<&[u8]>) -> Result<u32, ReError> {
    let mut buf = [0u8; 3];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], 0]))
}

fn read_u24_be(cursor: &mut Cursor<&[u8]>) -> Result<u32, ReError> {
    let mut buf = [0u8; 3];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u32::from_be_bytes([0, buf[0], buf[1], buf[2]]))
}

fn read_i24_le(cursor: &mut Cursor<&[u8]>) -> Result<i32, ReError> {
    let mut buf = [0u8; 3];
    std::io::Read::read_exact(cursor, &mut buf)?;
    let unsigned = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]);
    // Sign extend from 24 bits to 32 bits
    let value = if unsigned & 0x800000 != 0 {
        (unsigned | 0xFF000000) as i32
    } else {
        unsigned as i32
    };
    Ok(value)
}

fn read_uint_le(cursor: &mut Cursor<&[u8]>, nbytes: usize) -> Result<u64, ReError> {
    if nbytes == 0 || nbytes > 8 {
        return Err(ReError::String(format!("Invalid byte count for uint: {}", nbytes)));
    }
    let mut buf = [0u8; 8];
    std::io::Read::read_exact(cursor, &mut buf[..nbytes])?;
    Ok(u64::from_le_bytes(buf))
}

fn read_uint_be(cursor: &mut Cursor<&[u8]>, nbytes: usize) -> Result<u64, ReError> {
    if nbytes == 0 || nbytes > 8 {
        return Err(ReError::String(format!("Invalid byte count for uint: {}", nbytes)));
    }
    let mut buf = [0u8; 8];
    std::io::Read::read_exact(cursor, &mut buf[..nbytes])?;
    let mut be_buf = [0u8; 8];
    be_buf[8-nbytes..].copy_from_slice(&buf[..nbytes]);
    Ok(u64::from_be_bytes(be_buf))
}

pub fn parse_string(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<String, ReError> {
    let length = if metadata < 256 {
        cursor.read_u8()? as usize
    } else {
        cursor.read_u16::<LittleEndian>()? as usize
    };
    read_string(cursor, length)
}

pub fn parse_bit(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Vec<bool>, ReError> {
    let length = (metadata >> 8) * 8 + (metadata & 0xFF);
    let bitmap = read_bitmap_big_endian(cursor, length as usize)?;
    Ok(bitmap)
}

pub fn parse_blob(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Vec<u8>, ReError> {
    let length = read_uint_le(cursor, metadata as usize)? as usize;
    let mut vec = vec![0; length];
    std::io::Read::read_exact(cursor, &mut vec)?;
    Ok(vec)
}

pub fn parse_year(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<u16, ReError> {
    Ok(1900 + cursor.read_u8()? as u16)
}

pub fn parse_date(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<Date, ReError> {
    let value = read_u24_le(cursor)?;

    // Bits 1-5 store the day. Bits 6-9 store the month. The remaining bits store the year.
    let day = value % (1 << 5);
    let month = (value >> 5) % (1 << 4);
    let year = value >> 9;

    Ok(Date {
        year: year as u16,
        month: month as u8,
        day: day as u8,
    })
}

pub fn parse_time(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<Time, ReError> {
    let mut value = (read_i24_le(cursor)? << 8) >> 8;

    if value < 0 {
        return Err(ReError::String(
            "Parsing negative TIME values is not supported in this version".to_string(),
        ));
    }

    let second = value % 100;
    value = value / 100;
    let minute = value % 100;
    value = value / 100;
    let hour = value;
    Ok(Time {
        hour: hour as i16,
        minute: minute as u8,
        second: second as u8,
        millis: 0,
    })
}

pub fn parse_time2(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<Time, ReError> {
    let value = read_u24_be(cursor)?;
    let millis = parse_fractional_part(cursor, metadata)? / 1000;

    let negative = ((value >> 23) & 1) == 0;
    if negative {
        // It looks like other similar clients don't parse TIME2 values properly
        // In negative time values both TIME and FSP are stored in reverse order
        // See https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/sql/log_event.cc#L2022
        // See https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/mysys/my_time.cc#L1784
        return Err(ReError::String(
            "Parsing negative TIME values is not supported in this version".to_string(),
        ));
    }

    // 1 bit sign. 1 bit unused. 10 bits hour. 6 bits minute. 6 bits second.
    let hour = (value >> 12) % (1 << 10);
    let minute = (value >> 6) % (1 << 6);
    let second = value % (1 << 6);

    Ok(Time {
        hour: hour as i16,
        minute: minute as u8,
        second: second as u8,
        millis: millis as u32,
    })
}

pub fn parse_date_time(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<DateTime, ReError> {
    let mut value = cursor.read_u64::<LittleEndian>()?;
    let second = value % 100;
    value = value / 100;
    let minute = value % 100;
    value = value / 100;
    let hour = value % 100;
    value = value / 100;
    let day = value % 100;
    value = value / 100;
    let month = value % 100;
    value = value / 100;
    let year = value;

    Ok(DateTime {
        year: year as u16,
        month: month as u8,
        day: day as u8,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        millis: 0,
    })
}

pub fn parse_date_time2(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<DateTime, ReError> {
    let value = read_uint_be(cursor, 5)?;
    let millis = parse_fractional_part(cursor, metadata)? / 1000;

    // 1 bit sign(always true). 17 bits year*13+month. 5 bits day. 5 bits hour. 6 bits minute. 6 bits second.
    let year_month = (value >> 22) % (1 << 17);
    let year = year_month / 13;
    let month = year_month % 13;
    let day = (value >> 17) % (1 << 5);
    let hour = (value >> 12) % (1 << 5);
    let minute = (value >> 6) % (1 << 6);
    let second = value % (1 << 6);

    Ok(DateTime {
        year: year as u16,
        month: month as u8,
        day: day as u8,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        millis: millis as u32,
    })
}

pub fn parse_timestamp(cursor: &mut Cursor<&[u8]>, _metadata: u16) -> Result<u64, ReError> {
    let seconds = cursor.read_u32::<LittleEndian>()? as u64;
    Ok(seconds * 1000)
}

pub fn parse_timestamp2(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<u64, ReError> {
    let seconds = cursor.read_u32::<BigEndian>()? as u64;
    let millisecond = parse_fractional_part(cursor, metadata)? / 1000;
    let timestamp = seconds * 1000 + millisecond;
    Ok(timestamp)
}

pub fn parse_fractional_part(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<u64, ReError> {
    let length = (metadata + 1) / 2;
    if length == 0 {
        return Ok(0);
    }

    let fraction = read_uint_be(cursor, length as usize)?;
    Ok(fraction * u64::pow(100, 3 - length as u32))
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
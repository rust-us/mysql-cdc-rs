use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Read};
use common::err::decode_error::ReError;

/// See <a href="https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html">Docs</a>

const DIGITS_PER_INT: u8 = 9;
const COMPRESSED_BYTES: [u8; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

pub fn parse_decimal(cursor: &mut Cursor<&[u8]>, metadata: u16) -> Result<String, ReError> {
    let (length, precision, scale, compressed_integral, compressed_fractional, uncompressed_integral, uncompressed_fractional) =
                            decimal_fractional(metadata);

    // Format
    // [1-3 bytes]  [4 bytes]      [4 bytes]        [4 bytes]      [4 bytes]      [1-3 bytes]
    // [Compressed] [Uncompressed] [Uncompressed] . [Uncompressed] [Uncompressed] [Compressed]
    let mut value = vec![0; length as usize];
    cursor.read_exact(&mut value)?;
    let mut result = String::new();

    let negative = (value[0] & 0x80) == 0;
    value[0] ^= 0x80;

    if negative {
        result += "-";
        for i in 0..value.len() {
            value[i] ^= 0xFF;
        }
    }

    let mut buffer = Cursor::new(value.as_slice());

    let mut started = false;
    let mut size = COMPRESSED_BYTES[compressed_integral as usize];

    if size > 0 {
        let number = buffer.read_uint::<BigEndian>(size as usize)? as u32;
        if number > 0 {
            started = true;
            result += &number.to_string();
        }
    }
    for _i in 0..uncompressed_integral {
        let number = buffer.read_u32::<BigEndian>()?;
        if started {
            result += &format!("{val:0prec$}", prec = 9, val = number)
        } else if number > 0 {
            started = true;
            result += &number.to_string();
        }
    }

    // There has to be at least 0
    if !started {
        result += "0";
    }
    if scale > 0 {
        result += ".";
    }

    size = COMPRESSED_BYTES[compressed_fractional as usize];
    for _i in 0..uncompressed_fractional {
        let value = buffer.read_u32::<BigEndian>()?;
        result += &format!("{val:0prec$}", prec = 9, val = value)
    }
    if size > 0 {
        let value = buffer.read_uint::<BigEndian>(size as usize)? as u32;
        let precision = compressed_fractional as usize;
        result += &format!("{val:0prec$}", prec = precision, val = value)
    }
    Ok(result)
}

///
///
/// # Arguments
///
/// * `metadata`:
///
/// returns: (u8, u8)
///
pub fn decimal_fractional(metadata: u16) -> (u8, u8, u8, u8, u8, u8, u8) {
    // precision 是表示有效数字数的精度。 P范围为1〜65。
    // D是表示小数点后的位数。 D的范围是0~30。MySQL要求D小于或等于(<=)P。
    let scale = (metadata & 0xFF) as u8;
    let precision = (metadata >> 8) as u8;

    let integral = if precision > scale {
        precision - scale
    } else {
        scale - precision
    };

    let uncompressed_integral = integral / DIGITS_PER_INT;
    let uncompressed_fractional = scale / DIGITS_PER_INT;

    let compressed_integral = integral - (uncompressed_integral * DIGITS_PER_INT);
    let compressed_fractional = scale - (uncompressed_fractional * DIGITS_PER_INT);

    let length = (uncompressed_integral << 2) //  uncompressed_integral * 4
        + COMPRESSED_BYTES[compressed_integral as usize]
        + (uncompressed_fractional << 2)
        + COMPRESSED_BYTES[compressed_fractional as usize];

    (length, precision, scale, compressed_integral, compressed_fractional, uncompressed_integral, uncompressed_fractional)
}

pub fn get_meta(precision: u16, scale:u8) -> u16 {
    let mut meta: u16 = precision << 8;
    meta += scale as u16;

    meta
}

/// 将 meta 拆成两个 u8, 表示precision、scale
pub fn get_scale(meta: u16) -> (u8, u8) {
    let scale = (meta & 0xFF) as u8;
    let precision = (meta >> 8) as u8;

    (precision, scale)
}

#[cfg(test)]
mod tests {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;
    use nom::combinator::map;
    use nom::number::complete::le_u8;
    use crate::row::decimal::{get_meta, get_scale, parse_decimal};

    // #[test]
    // fn parse_positive_number() {
    //     // decimal(65,10), column = '1234567890112233445566778899001112223334445556667778889.9900011112'
    //     let payload: Vec<u8> = vec![
    //         65, 10, 129, 13, 251, 56, 210, 6, 176, 139, 229, 33, 200, 92, 19, 0, 16, 248, 159, 19,
    //         239, 59, 244, 39, 205, 127, 73, 59, 2, 55, 215, 2,
    //     ];
    //     let mut cursor = Cursor::new(payload.as_slice());
    //     let metadata = cursor.read_u16::<LittleEndian>().unwrap();
    //
    //     let expected =
    //         String::from("1234567890112233445566778899001112223334445556667778889.9900011112");
    //     assert_eq!(expected, parse_decimal(&mut cursor, metadata).unwrap());
    // }
    //
    // #[test]
    // fn parse_negative_number() {
    //     // decimal(65,10), column = '-1234567890112233445566778899001112223334445556667778889.9900011112'
    //     let payload: Vec<u8> = vec![
    //         65, 10, 126, 242, 4, 199, 45, 249, 79, 116, 26, 222, 55, 163, 236, 255, 239, 7, 96,
    //         236, 16, 196, 11, 216, 50, 128, 182, 196, 253, 200, 40, 253,
    //     ];
    //     let mut cursor = Cursor::new(payload.as_slice());
    //     let metadata = cursor.read_u16::<LittleEndian>().unwrap();
    //
    //     let expected =
    //         String::from("-1234567890112233445566778899001112223334445556667778889.9900011112");
    //     assert_eq!(expected, parse_decimal(&mut cursor, metadata).unwrap());
    // }
    //
    // #[test]
    // fn parse_with_starting_zeros_ignored() {
    //     // decimal(65,10), column = '7778889.9900011112'
    //     let payload: Vec<u8> = vec![
    //         65, 10, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 118, 178,
    //         73, 59, 2, 55, 215, 2,
    //     ];
    //     let mut cursor = Cursor::new(payload.as_slice());
    //     let metadata = cursor.read_u16::<LittleEndian>().unwrap();
    //
    //     let expected = String::from("7778889.9900011112");
    //     assert_eq!(expected, parse_decimal(&mut cursor, metadata).unwrap());
    // }
    //
    // #[test]
    // fn parse_with_integral_zero() {
    //     // decimal(65,10), column = '.9900011112'
    //     let payload: Vec<u8> = vec![
    //         65, 10, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    //         59, 2, 55, 215, 2,
    //     ];
    //     let mut cursor = Cursor::new(payload.as_slice());
    //     let metadata = cursor.read_u16::<LittleEndian>().unwrap();
    //
    //     let expected = String::from("0.9900011112");
    //     assert_eq!(expected, parse_decimal(&mut cursor, metadata).unwrap());
    // }
    //
    // #[test]
    // fn compressed_fractional_starting_zeros_preserved() {
    //     // In this test first two zeros are preserved->[uncompr][comp]
    //     // decimal(60,15), column = '34445556667778889.123456789006700'
    //     let payload: Vec<u8> = vec![
    //         60, 15, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 13, 152, 244, 39, 205, 127, 73, 7, 91,
    //         205, 21, 0, 26, 44,
    //     ];
    //     let mut cursor = Cursor::new(payload.as_slice());
    //     let metadata = cursor.read_u16::<LittleEndian>().unwrap();
    //
    //     let expected = String::from("34445556667778889.123456789006700");
    //     assert_eq!(expected, parse_decimal(&mut cursor, metadata).unwrap());
    // }
    //
    // #[test]
    // fn parse_integer() {
    //     // decimal(60,0), column = '34445556667778889'
    //     let payload: Vec<u8> = vec![
    //         60, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 13, 152, 244, 39,
    //         205, 127, 73,
    //     ];
    //     let mut cursor = Cursor::new(payload.as_slice());
    //     let metadata = cursor.read_u16::<LittleEndian>().unwrap();
    //
    //     let expected = String::from("34445556667778889");
    //     assert_eq!(expected, parse_decimal(&mut cursor, metadata).unwrap());
    // }

    #[test]
    fn test_parse_meta() {
        let precision = 10;
        let scale = 4u8;

        let m1 = get_meta(precision, scale);
        let (p1, s1) = get_scale(m1);
        assert_eq!(precision, p1 as u16);
        assert_eq!(scale, s1);

        let precision2 = 50;
        let scale2 = 30;

        let m2 = get_meta(precision2, scale2);
        let (p2, s2) = get_scale(m2);
        assert_eq!(precision2, p2 as u16);
        assert_eq!(scale2, s2);

        let metadata_ = 12830u16;
        let (pp, ss) = get_scale(metadata_);
        assert_eq!(30, ss as u16);
        assert_eq!(50, pp);

        assert_eq!(3330, get_meta(13, 2));
        let (a, b) = get_scale(3330);
        assert_eq!(13, a);
        assert_eq!(2, b);

        assert_eq!(3075, get_meta(12, 3));
        let (a, b) = get_scale(3075);
        assert_eq!(12, a);
        assert_eq!(3, b);

        assert_eq!(2561, get_meta(10, 1));
        let (a, b) = get_scale(2561);
        assert_eq!(10, a);
        assert_eq!(1, b);

        assert_eq!(3073, get_meta(12, 1));
        let (a, b) = get_scale(3073);
        assert_eq!(12, a);
        assert_eq!(1, b);

        let (a, b) = get_scale(3075);
        assert_eq!(12, a);
        assert_eq!(3, b);
    }
}

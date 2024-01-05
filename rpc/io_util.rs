use std::mem::transmute;
use std::slice;

use byteorder::{BigEndian, ByteOrder};

use crate::error::{err_if, HResult};
use crate::error::Error::RaftCommandParseUtf8Err;

const NULL_LENGTH: i16 = -1;
const KEPLER_LENGTH: i16 = -2;

pub fn op_read_utf8(buf: &mut [i8]) -> HResult<(Option<String>, usize)> {
    let buf: &mut [u8] = unsafe { transmute(buf) };
    let len = BigEndian::read_i16(buf);
    let mut pos = 2;
    if len == NULL_LENGTH {
        return Ok((None, pos));
    } else if len == KEPLER_LENGTH {
        let str_len = (BigEndian::read_i32(&buf[pos..]) as usize);
        pos += 4;
        err_if!(RaftCommandParseUtf8Err, str_len*2+pos > buf.len());
        // java utf8 is BigEndian, rust utf16 is LittleEndian.
        let c_bytes = &mut buf[pos..(pos + str_len * 2)];
        // swap low/high bytes in 2 bytes
        for i in 0..str_len {
            let idx = i * 2;
            let l = c_bytes[idx];
            c_bytes[idx] = c_bytes[idx + 1];
            c_bytes[idx + 1] = l;
        }
        let u16s = unsafe {
            slice::from_raw_parts(c_bytes.as_ptr() as *const u16, str_len)
        };
        let s = String::from_utf16(u16s).unwrap();
        pos += str_len * 2;
        Ok((Some(s), pos))
    } else {
        err_if!(RaftCommandParseUtf8Err, len > 0);
        let buf = &buf[pos..(pos + len as usize)];
        let s = String::from_utf8(Vec::from(buf)).map_err(|_| RaftCommandParseUtf8Err)?;
        pos += len as usize;
        Ok((Some(s), pos))
    }
}

#[cfg(test)]
mod test {
    use crate::io_util::op_read_utf8;

    #[test]
    fn test0() {
        let mut data = [-1, -2, 0, 0, 0, 5, 0, 49, 0, 50, 0, 51, 0, 52, 0, 53];
        let (s, len) = op_read_utf8(data.as_mut_slice()).unwrap();
        println!("str: {}, len: {}", s.as_ref().unwrap(), len);
        assert_eq!("12345", s.unwrap());
        assert_eq!(16, len);
    }

    #[test]
    fn test1() {
        let mut data = [-1, -2, 0, 0, 0, 42, 0, 49, 0, 50, 0, 51, 0, 52, 0, 53, 0, 54, 0, 55, 0, 56, 0, 57, 0, 48, 0, 95,
            0, 43, 0, 65, 0, 66, 0, 67, 0, 68, 0, 69, 0, 70, 0, 71, 0, 72, 0, 73, 0, 97, 0, 115, 0, 100, 0, 107, 0, 107, 0, 106, 0,
            105, 0, 113, 0, 58, 0, 60, 0, 62, 0, 63, 0, 41, 0, 64, 0, 40, 0, 35, 0, 42, 0, 36, 0, 38, 0, 36, 0, 34];
        let (s, len) = op_read_utf8(data.as_mut_slice()).unwrap();
        println!("str: {}, len: {}", s.as_ref().unwrap(), len);
        assert_eq!("1234567890_+ABCDEFGHIasdkkjiq:<>?)@(#*$&$\"", s.unwrap());
        assert_eq!(90, len);
    }

    #[test]
    fn test2() {
        let mut data = [-1, -2, 0, 0, 0, 1, 81, 118];
        let (s, len) = op_read_utf8(&mut data).unwrap();
        println!("str: {}, len: {}", s.as_ref().unwrap(), len);
        assert_eq!("其", s.unwrap());
        assert_eq!(len, 8);
    }

    #[test]
    fn test3() {
        let mut data = [-1, -2, 0, 0, 0, 76, 81, 118, 91, -98, 83, -22, 101, 47, 99, 1, -126, -15, 101, -121, 92,
            49, 89, 125, 78, -122, -1, 12, 0, 114, 0, 97, 0, 102, 0, 116, 104, 70, 103, -74, 78, 45, 108, -95, 117, 40, 82,
            48, 78, 45, 101, -121, 48, 2, 83, -22, 102, 47, 87, 40, 0, 32, 0, 112, 0, 97, 0, 121, 0, 108, 0, 111, 0, 97, 0,
            100, 0, 32, 78, 45, 117, 40, 82, 48, 78, -122, 48, 2, 78, 13, -113, -57, 98, 17, 78, -20, 79, 26, 101, -80, 80,
            90, 0, 67, 0, 111, 0, 109, 0, 109, 0, 97, 0, 110, 0, 100, 127, 22, 120, 1, 101, -71, 95, 15, -1, 12, 78, 13, 79,
            26, 117, 40, 78, 75, 82, 77, 118, -124, 0, 32, 0, 119, 0, 114, 0, 105, 0, 116, 0, 101, 0, 85, 0, 84, 0, 70, 0, 56];
        let (s, len) = op_read_utf8(data.as_mut_slice()).unwrap();
        println!("str: {}, len: {}", s.as_ref().unwrap(), len);
        assert_eq!("其实只支持英文就好了，raft框架中没用到中文。只是在 payload 中用到了。不过我们会新做Command编码方式，不会用之前的 writeUTF8", s.unwrap());
        assert_eq!(len, 158);
    }
}
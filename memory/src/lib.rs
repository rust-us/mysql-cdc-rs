#![feature(allocator_api)]
#![feature(slice_ptr_get)]

use std::{ptr, slice};
use std::alloc::{Allocator, AllocError, Global, Layout};
use std::fmt::{Display, Formatter, Pointer};
use std::ptr::NonNull;

use paste::paste;

// default 4k per page
const DEFAULT_SEGMENT_LEN: usize = 4096;

const NULL_LENGTH: i16 = -1;
const KEPLER_LENGTH: i16 = -2;

pub struct Segment {
    ptr: NonNull<[u8]>,
}

pub struct Buffer {
    segments: Vec<Segment>,
    seg_idx: usize,
    seg_offset: usize,
    capacity: usize,
}

unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Segment {
    fn new() -> Result<Self, AllocError> {
        let ptr = unsafe {
            let g = Global {};
            let layout = Layout::from_size_align_unchecked(DEFAULT_SEGMENT_LEN, 8);
            g.allocate(layout)?
        };
        Ok(Self { ptr })
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            self.ptr.as_ref()
        }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut[u8] {
        unsafe {
            self.ptr.as_mut()
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let g = Global{};
        unsafe {
            let layout = Layout::from_size_align_unchecked(DEFAULT_SEGMENT_LEN, 8);
            g.deallocate(self.ptr.as_non_null_ptr(), layout);
        }

    }
}

impl Buffer {
    #[inline]
    pub fn new() -> Result<Self, AllocError> {
        Ok(Self {
            segments: vec![],
            seg_idx: 0,
            seg_offset: 0,
            capacity: 0,
        })
    }

    pub fn ensure_cap(&mut self, extra: usize) -> Result<(), AllocError> {
        if DEFAULT_SEGMENT_LEN * self.seg_idx + self.seg_offset + extra >= self.capacity {
            self.scale_out(1)?;
        }
        Ok(())
    }

    pub fn write_byte(&mut self, value: i8) -> Result<usize, AllocError> {
        self.ensure_cap(1)?;
        let seg = self.current_segment();
        unsafe {
            seg.ptr.as_mut_ptr().offset(self.seg_offset as isize).write(value as u8);
        }
        self.seg_offset += 1;
        Ok(1)
    }

    pub fn write_i24(&mut self, value: i32) -> Result<usize, AllocError> {
        let mut bytes = value.to_be_bytes();
        // the highest bit is sign bit, remove the highest bit to i24
        bytes[1] &= bytes[0] | 0b0111_1111;
        // big endian, take last 3 bytes
        self.write_bytes(&bytes[1..4])
    }

    pub fn write_u24(&mut self, value: i32) -> Result<usize, AllocError> {
        let bytes = value.to_be_bytes();
        // big endian, take last 3 bytes
        self.write_bytes(&bytes[1..4])
    }

    #[inline]
    pub fn write_bool(&mut self, value: bool) -> Result<usize, AllocError> {
        self.write_byte(value as i8)
    }

    pub fn write_utf8(&mut self, str: &str) -> Result<usize, AllocError> {
        if str.len() == 0 {
            return self.write_utf8_null();
        }
        let mut pos = 0;
        pos += self.write_short(KEPLER_LENGTH)?;
        let chars: Vec<u16> = str.encode_utf16().map(|c|c.swap_bytes()).collect();
        pos += self.write_int(chars.len() as _)?;
        let bytes = unsafe {
            slice::from_raw_parts(chars.as_ptr() as *const u8, chars.len() * 2)
        };
        pos += self.write_bytes(bytes)?;
        Ok(pos)
    }

    #[inline]
    pub fn write_utf8_null(&mut self) -> Result<usize, AllocError> {
        self.write_short(NULL_LENGTH)
    }

    pub fn write_bytes(&mut self, value: &[u8]) -> Result<usize, AllocError> {
        assert!(value.len() > 0, "value bytes must not be empty!");
        let mut left = value.len();
        self.ensure_cap(left)?;

        let cp_len = left.min(DEFAULT_SEGMENT_LEN - self.seg_offset);
        let seg = self.current_segment();
        let mut src = value.as_ptr();
        unsafe {
            let dst = seg.ptr.as_mut_ptr().offset(self.seg_offset as isize);
            ptr::copy(src, dst, cp_len);
        }
        left -= cp_len;
        if left == 0 {
            self.seg_offset += cp_len;
            return Ok(value.len());
        }
        unsafe {
            while left > 0 {
                self.seg_idx += 1;
                src = src.offset(cp_len as isize);
                let cp_len = left.min(DEFAULT_SEGMENT_LEN);
                let seg = self.current_segment();
                let dst = seg.ptr.as_mut_ptr();
                ptr::copy(src, dst, cp_len);
                if left < DEFAULT_SEGMENT_LEN {
                    // this is final loop
                    self.seg_offset = left;
                }
                left -= cp_len;
            }
        }
        Ok(value.len())
    }

    fn scale_out(&mut self, segment_count: usize) -> Result<(), AllocError> {
        for _ in 0..segment_count {
            let segment = Segment::new()?;
            self.segments.push(segment);
        }
        self.capacity += DEFAULT_SEGMENT_LEN * segment_count;
        if self.seg_offset == DEFAULT_SEGMENT_LEN {
            self.seg_idx += 1;
            self.seg_offset = 0;
        }
        Ok(())
    }

    #[inline]
    pub fn current_segment(&mut self) -> &mut Segment {
        &mut self.segments[self.seg_idx]
    }

    pub fn segment(&self, i: usize) -> &Segment {
        &self.segments[i]
    }

    pub fn copy_slice(&self) -> Vec<u8> {
        let len = self.seg_idx * DEFAULT_SEGMENT_LEN + self.seg_offset;
        if len == 0 {
            return Vec::new();
        }
        let mut dst: Vec<u8> = Vec::with_capacity(len);
        unsafe {
            dst.set_len(len);
        }
        let mut offset: isize = 0;
        let dst_p = dst.as_mut_ptr();
        for i in 0..=self.seg_idx {
            let seg = &self.segments[i];
            if i < self.seg_idx {
                unsafe {
                    ptr::copy(seg.ptr.as_mut_ptr(), dst_p.offset(offset), DEFAULT_SEGMENT_LEN);
                }
                offset += DEFAULT_SEGMENT_LEN as isize;
            } else {
                unsafe {
                    ptr::copy(seg.ptr.as_mut_ptr(), dst_p.offset(offset), self.seg_offset);
                }
            }
        }
        dst
    }

    pub fn skip_bytes(&mut self, n: usize) -> Result<usize, AllocError> {
        self.ensure_cap(n)?;
        self.seg_offset += n;
        if self.seg_offset > DEFAULT_SEGMENT_LEN {
            self.seg_idx += 1;
            self.seg_offset -= DEFAULT_SEGMENT_LEN;
        }
        Ok(n)
    }

    #[inline]
    pub fn position(&self) -> (usize, usize) {
        return (self.seg_idx, self.seg_offset)
    }

    #[inline]
    pub fn reset(&mut self) {
        self.seg_idx = 0;
        self.seg_offset = 0;
    }

    #[inline]
    pub unsafe fn set_position(&mut self, seg_idx: usize, seg_offset: usize) {
        self.seg_idx = seg_idx;
        self.seg_offset = seg_offset;
    }

    #[inline]
    pub fn length(&self) -> usize {
        self.seg_idx * DEFAULT_SEGMENT_LEN + self.seg_offset
    }

}

macro_rules! def_func {
    ($($name: ident, $t: ty); *) => {
        paste! {
            impl Buffer {
                $(
                #[inline]
                pub fn [<write_ $name>](&mut self, value: $t) -> Result<usize, AllocError> {
                    self.write_bytes(value.to_be_bytes().as_slice())
                }
                )*
            }
        }
    };
}

// 生成基本数据类型的写入方法
def_func!(
    short,  i16;
    int,    i32;
    long,   i64;
    float,  f32;
    double, f64
);

impl Display for Buffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "seg_count: {}, seg_idx: {}, seg_offset: {}\n", self.segments.len(), self.seg_idx, self.seg_offset)?;
        &self.segments.iter().enumerate().for_each(|(i, seg)| {
            let _ = write!(f, "seg{}\n\t", i);
            let p = seg.ptr.as_mut_ptr();
            for j in 0..DEFAULT_SEGMENT_LEN {
                unsafe {
                    let _ = write!(f, "{},", p.offset(j as _).read() as i8);
                }
            }
            let _ = write!(f, "\n");
        });
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::mem::transmute;

    use crate::Buffer;

    #[test]
    fn test_utf8() {
        let mut buffer = Buffer::new().unwrap();
        buffer.write_utf8("12345").unwrap();
        let dst = buffer.copy_slice();
        let expect = [-1_i8,-2,0,0,0,5,0,49,0,50,0,51,0,52,0,53];
        let buffer: &[i8] = unsafe {
            transmute(dst.as_slice())
        };
        assert_eq!(expect.as_slice(), buffer);
    }

    #[test]
    fn test_utf8_1() {
        let mut buffer = Buffer::new().unwrap();
        buffer.write_utf8("1234567890_+ABCDEFGHIasdkkjiq:<>?)@(#*$&$\"").unwrap();
        let dst = buffer.copy_slice();
        let expect = [-1_i8,-2,0,0,0,42,0,49,0,50,0,51,0,52,0,53,0,54,0,55,0,56,0,57,0,48,0,95,0,43,0,65,0,66,0,
            67,0,68,0,69,0,70,0,71,0,72,0,73,0,97,0,115,0,100,0,107,0,107,0,106,0,105,0,113,0,58,0,60,0,62,0,63,0,41,
            0,64,0,40,0,35,0,42,0,36,0,38,0,36,0,34];
        let buffer: &[i8] = unsafe {
            transmute(dst.as_slice())
        };
        assert_eq!(expect.as_slice(), buffer);
    }

    #[test]
    fn test_utf8_2() {
        let mut buffer = Buffer::new().unwrap();
        buffer.write_utf8("其").unwrap();
        let dst = buffer.copy_slice();
        let expect = [-1_i8,-2,0,0,0,1,81,118];
        let buffer: &[i8] = unsafe {
            transmute(dst.as_slice())
        };
        assert_eq!(expect.as_slice(), buffer);
    }

    #[test]
    fn test_utf8_3() {
        let mut buffer = Buffer::new().unwrap();
        buffer.write_utf8("其实只支持英文就好了，raft框架中没用到中文。只是在 payload 中用到了。不过我们会新做Command编码方式，不会用之前的 writeUTF8").unwrap();
        let dst = buffer.copy_slice();
        let expect = [-1_i8,-2,0,0,0,76,81,118,91,-98,83,-22,101,47,99,1,-126,-15,101,-121,92,49,89,125,78,-122,
            -1,12,0,114,0,97,0,102,0,116,104,70,103,-74,78,45,108,-95,117,40,82,48,78,45,101,-121,48,2,83,-22,102,47,
            87,40,0,32,0,112,0,97,0,121,0,108,0,111,0,97,0,100,0,32,78,45,117,40,82,48,78,-122,48,2,78,13,-113,-57,98,
            17,78,-20,79,26,101,-80,80,90,0,67,0,111,0,109,0,109,0,97,0,110,0,100,127,22,120,1,101,-71,95,15,-1,12,78,
            13,79,26,117,40,78,75,82,77,118,-124,0,32,0,119,0,114,0,105,0,116,0,101,0,85,0,84,0,70,0,56];
        let buffer: &[i8] = unsafe {
            transmute(dst.as_slice())
        };
        assert_eq!(expect.as_slice(), buffer);
    }

}
#![feature(allocator_api)]
#![feature(slice_ptr_get)]

use std::{ptr, slice};
use std::alloc::{Allocator, AllocError, Global, Layout};
use std::fmt::{Debug, Display, Formatter};
use std::mem::{size_of, transmute};
use std::ptr::NonNull;

use crc32fast::Hasher;
use paste::paste;

// default 4k per page
const DEFAULT_SEGMENT_LEN: usize = 4096;

const NULL_LENGTH: i16 = -1;
const KEPLER_LENGTH: i16 = -2;

#[derive(Debug)]
pub struct Segment {
    ptr: NonNull<[u8]>,
}

#[derive(Debug)]
pub struct Buffer {
    segments: Vec<Segment>,
    seg_idx: usize,
    seg_offset: usize,
    capacity: usize,
}

/// The immutable buffer, for read only
#[derive(Debug)]
pub struct ImmutableBuffer<'a> {
    segments: &'a Vec<Segment>,
    // read segment index
    seg_idx: usize,
    // read segment offset
    seg_offset: usize,
    length: usize,
    capacity: usize,
}

pub enum Error {
    ReadOutOfRange,
}

/// This is thread unsafe!
pub struct IterFixLenMut<'a> {
    segments: &'a mut [Segment],
    max_idx: usize,
    max_offset: usize,
    cur_idx: usize,
}

pub struct IterFixLen<'a> {
    segments: &'a [Segment],
    max_idx: usize,
    max_offset: usize,
    cur_idx: usize,
}

unsafe impl Send for Segment {}

unsafe impl Sync for Segment {}

unsafe impl Send for Buffer {}

unsafe impl Sync for Buffer {}

unsafe impl<'a> Send for ImmutableBuffer<'a> {}

unsafe impl<'a> Sync for ImmutableBuffer<'a> {}

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
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            self.ptr.as_mut()
        }
    }

    /// read T from segment with offset
    /// # Unsafe
    /// offset should not out of range
    #[inline]
    unsafe fn read_n<const N: usize>(&self, offset: isize) -> [u8; N] {
        let p = self.as_slice().as_ptr().offset(offset);
        (p as *const [u8; N]).read()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let g = Global {};
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

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn ensure_cap(&mut self, extra: usize) -> Result<(), AllocError> {
        if DEFAULT_SEGMENT_LEN * self.seg_idx + self.seg_offset + extra > self.capacity {
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

    pub fn write_i32(&mut self, value: i32) -> Result<usize, AllocError> {
        let mut bytes = value.to_be_bytes();
        self.write_bytes(&bytes[..])
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
        let chars: Vec<u16> = str.encode_utf16().map(|c| c.swap_bytes()).collect();
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

    pub fn crc(&self, start: usize, end: usize) -> u32 {
        let start_seg_idx = start / DEFAULT_SEGMENT_LEN;
        let start_seg_offset = start % DEFAULT_SEGMENT_LEN;
        let end_seg_idx = end / DEFAULT_SEGMENT_LEN;
        let end_seg_offset = end % DEFAULT_SEGMENT_LEN;

        let mut h = Hasher::new();
        for i in start_seg_idx ..= end_seg_idx {
            let l = if i == start_seg_idx {start_seg_offset} else { 0 };
            let r = if i == end_seg_idx {end_seg_offset} else { DEFAULT_SEGMENT_LEN };
            let seg = &self.segments[i];
            let ptr = &seg.as_slice()[l..r];
            h.update(ptr);
        }
        h.finalize()
    }

    pub unsafe fn append_buf(&mut self, other_buf: &Buffer) -> Result<usize, AllocError> {
        for i in 0..=other_buf.seg_idx {
            let seg = &other_buf.segments[i];
            if i < other_buf.seg_idx {
                self.write_bytes(seg.ptr.as_ref())?;
            } else {
                self.write_bytes(&seg.ptr.as_ref()[0..other_buf.seg_offset])?;
            }
        }
        Ok(other_buf.length())
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
        return (self.seg_idx, self.seg_offset);
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

    pub unsafe fn set_length(&mut self, len: usize) {
        let mut seg_idx = len / DEFAULT_SEGMENT_LEN;
        let seg_offset = len % DEFAULT_SEGMENT_LEN;
        self.set_position(seg_idx, seg_offset);
    }

    #[inline]
    pub fn length(&self) -> usize {
        self.seg_idx * DEFAULT_SEGMENT_LEN + self.seg_offset
    }

    #[inline]
    pub fn iter_fix_len_mut(&mut self, fix_len: usize) -> IterFixLenMut<'_> {
        let mut max_idx = fix_len / DEFAULT_SEGMENT_LEN;
        let max_offset = fix_len % DEFAULT_SEGMENT_LEN;
        if max_offset > 0 {
            max_idx += 1;
        }
        IterFixLenMut {
            segments: &mut self.segments[0..max_idx],
            max_idx,
            max_offset,
            cur_idx: 0,
        }
    }

    #[inline]
    pub fn iter_fix_len(&self, fix_len: usize) -> IterFixLen<'_> {
        iter_fix_len_0(&self.segments, fix_len)
    }

    #[inline]
    pub fn as_immutable(&self) -> ImmutableBuffer {
        let length = self.length();
        ImmutableBuffer {
            segments: &self.segments,
            seg_idx: 0,
            seg_offset: 0,
            length,
            capacity: self.capacity,
        }
    }
}

impl<'a> Iterator for IterFixLenMut<'a> {
    type Item = &'a mut [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx >= self.segments.len() {
            return None;
        }
        let is_last_seg = self.cur_idx + 1 == self.segments.len();

        let item = self.segments
            .get_mut(self.cur_idx)
            .map(|s| {
                let s = if is_last_seg {
                    &mut s.as_mut_slice()[0..self.max_offset]
                } else {
                    s.as_mut_slice()
                };
                unsafe {
                    // WARN!, does this cause memory leak?
                    &mut *(s as *const [u8] as *mut [u8])
                }
            });
        self.cur_idx += 1;
        item
    }
}

impl<'a> Iterator for IterFixLen<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx >= self.segments.len() {
            return None;
        }
        let is_last_seg = self.cur_idx + 1 == self.segments.len();

        let item = self.segments
            .get(self.cur_idx)
            .map(|s| {
                if is_last_seg {
                    &(s.as_slice()[0..self.max_offset])
                } else {
                    s.as_slice()
                }
            });
        self.cur_idx += 1;
        item
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

macro_rules! check_capacity {
    ($self: ident, $addition: expr) => {
        if ($self.seg_offset + $addition >= $self.capacity) {
            return Err(Error::ReadOutOfRange);
        }
    };
}

impl<'a> ImmutableBuffer<'a> {
    #[inline]
    pub fn current_segment(&self) -> &'a Segment {
        &self.segments[self.seg_idx]
    }

    #[inline]
    fn add_offset(&mut self, offset: usize) {
        self.seg_offset += offset;
        if self.seg_offset >= DEFAULT_SEGMENT_LEN {
            self.seg_idx += 1;
            self.seg_offset -= DEFAULT_SEGMENT_LEN;
        }
    }

    #[inline]
    pub fn read_byte(&mut self) -> Result<i8, Error> {
        check_capacity!(self, 1);
        let seg = self.current_segment();
        let r = seg.as_slice()[self.seg_offset];
        self.add_offset(1);
        Ok(r as i8)
    }

    pub fn read_vec(&mut self, mut len: usize) -> Result<Vec<u8>, Error> {
        if len == 0 {
            return Ok(vec![]);
        }
        check_capacity!(self, len);
        let mut v: Vec<u8> = Vec::with_capacity(len);
        unsafe { v.set_len(len); };
        // the raw pointer of vec
        let p = v.as_mut_ptr();
        if self.seg_offset + len < DEFAULT_SEGMENT_LEN {
            // fast branch
            let seg = self.current_segment();
            let s = seg.as_slice();
            unsafe {
                ptr::copy(
                    s.as_ptr().offset(self.seg_offset as _),
                    p,
                    len,
                )
            };
            self.add_offset(len);
        } else {
            // data cross multi segment
            // 1. copy first segment's left data
            let s = self.current_segment().as_slice();
            let len0 = DEFAULT_SEGMENT_LEN - self.seg_offset;
            unsafe {
                ptr::copy(
                    s.as_ptr().offset(self.seg_offset as _),
                    p,
                    len0,
                );
            };
            // 2. copy middle segments
            len -= len0;
            let mut p_offset = len0;
            let count = len % DEFAULT_SEGMENT_LEN;
            self.seg_idx += 1;
            for _ in 0..count {
                let s = self.current_segment().as_slice();
                unsafe {
                    ptr::copy(
                        s.as_ptr(),
                        p.offset(p_offset as _),
                        DEFAULT_SEGMENT_LEN,
                    )
                };
                self.seg_idx += 1;
                p_offset += DEFAULT_SEGMENT_LEN;
            }
            // 3. copy last segment's tail data
            let len1 = len / DEFAULT_SEGMENT_LEN;
            if len1 > 0 {
                let s = self.current_segment().as_slice();
                unsafe {
                    ptr::copy(
                        s.as_ptr(),
                        p.offset(p_offset as _),
                        len1,
                    );
                };
            };
        }
        Ok(v)
    }

    fn read_n<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        check_capacity!(self, N);
        let mut data = [0_u8; N];
        if self.seg_offset + N < DEFAULT_SEGMENT_LEN {
            let seg = self.current_segment();
            // big endian, read data to high 3 bytes
            let s = seg.as_slice();
            for i in 0..N {
                data[i] = s[self.seg_offset + i];
            }
            self.add_offset(N);
        } else {
            // cross 2 segments
            let seg = self.current_segment();
            let s = seg.as_slice();
            let mut idx = 0;
            for i in self.seg_offset..DEFAULT_SEGMENT_LEN {
                data[idx] = s[i];
                idx += 1;
            }
            let read_len1 = self.seg_offset + 3 - DEFAULT_SEGMENT_LEN;
            self.seg_idx += 1;
            let seg = self.current_segment();
            let s = seg.as_slice();
            for i in 0..read_len1 {
                data[idx] = s[i];
                idx += 1;
            }
            self.seg_offset = read_len1;
        }
        Ok(data)
    }

    pub fn read_i24(&mut self) -> Result<i32, Error> {
        let d: [u8; 3] = self.read_n()?;
        let v = ((d[0] as i32) << 16) | ((d[1] as u32 as i32) << 8) | (d[0] as u32 as i32);
        Ok(v)
    }

    pub fn read_u24(&mut self) -> Result<u32, Error> {
        let d: [u8; 3] = self.read_n()?;
        let v = ((d[0] as u32) << 16) | ((d[1] as u32) << 8) | (d[0] as u32);
        Ok(v)
    }

    #[inline]
    pub fn iter_fix_len(&self, fix_len: usize) -> IterFixLen<'_> {
        iter_fix_len_0(self.segments, fix_len)
    }

}

fn iter_fix_len_0(segments: &Vec<Segment>, fix_len: usize) -> IterFixLen {
    let mut max_idx = fix_len / DEFAULT_SEGMENT_LEN;
    let max_offset = fix_len % DEFAULT_SEGMENT_LEN;
    if max_offset > 0 {
        max_idx += 1;
    }
    IterFixLen {
        segments: &segments[0..max_idx],
        max_idx,
        max_offset,
        cur_idx: 0,
    }
}

impl TryFrom<&[u8]> for Buffer {
    type Error = AllocError;
    fn try_from(value: &[u8]) -> Result<Self, AllocError> {
        let mut buf = Buffer::new()?;
        buf.write_bytes(value)?;
        Ok(buf)
    }
}

impl <'a> Display for ImmutableBuffer<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "segments count: {}, length: {}, capacity: {}\n",
               self.segments.len(),
               self.length,
               self.capacity)?;
        for s in self.iter_fix_len(self.length) {
            let s1: &[i8] = unsafe { transmute(s) };
            write!(f, "{:?}\n", s1)?;
        }
        Ok(())
    }
}

macro_rules! def_func2 {
    ($($name: ident, $t: ty, $n: expr); *) => {
        paste! {
            impl <'a> ImmutableBuffer<'a> {
                $(
                pub fn [<read_ $name>](&mut self) -> Result<$t, Error> {
                    let n = size_of::<$t>();
                    check_capacity!(self, n);
                    if self.seg_offset + n < DEFAULT_SEGMENT_LEN {
                        // read in one segment, fast branch
                        let seg = self.current_segment();
                        let data = unsafe { seg.read_n(self.seg_offset as _) };
                        self.seg_offset += n;
                        return Ok($t::from_be_bytes(data));
                    }
                    assert!(n <= DEFAULT_SEGMENT_LEN, "this function not allow read to large data on stack");
                    // read cross two segments
                    // 1. read tail of the current segment
                    let tmp = [0_u8; size_of::<$t>()];
                    let dst = tmp.as_ptr() as *mut u8;
                    let read_len0 = DEFAULT_SEGMENT_LEN - self.seg_offset;
                    let seg = self.current_segment();
                    unsafe {
                        // copy tail of the current segment to dst
                        ptr::copy(
                            seg.as_slice().as_ptr().offset(self.seg_offset as isize),
                            dst,
                            read_len0,
                        );
                    };
                    // segment point to next one and reset offset
                    self.seg_idx += 1;
                    let read_len1 = n - read_len0;
                    self.seg_offset = read_len1;
                    let seg = self.current_segment();
                    unsafe {
                        // copy the left bytes
                        ptr::copy(
                            seg.as_slice().as_ptr(),
                            dst.offset(read_len0 as _),
                            read_len1,
                        )
                    };
                    Ok($t::from_be_bytes(tmp))
                }
                )*
            }
        }
    };
}

def_func2!(
    short,      i16, 2;
    u16,        u16, 2;
    int,        i32, 4;
    u32,        u32, 4;
    long,       i64, 8;
    float,      f32, 4;
    double,     f64, 8
);

#[cfg(test)]
mod test {
    use std::mem::transmute;

    use crate::Buffer;

    #[test]
    fn test_utf8() {
        let mut buffer = Buffer::new().unwrap();
        buffer.write_utf8("12345").unwrap();
        let dst = buffer.copy_slice();
        let expect = [-1_i8, -2, 0, 0, 0, 5, 0, 49, 0, 50, 0, 51, 0, 52, 0, 53];
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
        let expect = [-1_i8, -2, 0, 0, 0, 42, 0, 49, 0, 50, 0, 51, 0, 52, 0, 53, 0, 54, 0, 55, 0, 56, 0, 57, 0, 48, 0, 95, 0, 43, 0, 65, 0, 66, 0,
            67, 0, 68, 0, 69, 0, 70, 0, 71, 0, 72, 0, 73, 0, 97, 0, 115, 0, 100, 0, 107, 0, 107, 0, 106, 0, 105, 0, 113, 0, 58, 0, 60, 0, 62, 0, 63, 0, 41,
            0, 64, 0, 40, 0, 35, 0, 42, 0, 36, 0, 38, 0, 36, 0, 34];
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
        let expect = [-1_i8, -2, 0, 0, 0, 1, 81, 118];
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
        let expect = [-1_i8, -2, 0, 0, 0, 76, 81, 118, 91, -98, 83, -22, 101, 47, 99, 1, -126, -15, 101, -121, 92, 49, 89, 125, 78, -122,
            -1, 12, 0, 114, 0, 97, 0, 102, 0, 116, 104, 70, 103, -74, 78, 45, 108, -95, 117, 40, 82, 48, 78, 45, 101, -121, 48, 2, 83, -22, 102, 47,
            87, 40, 0, 32, 0, 112, 0, 97, 0, 121, 0, 108, 0, 111, 0, 97, 0, 100, 0, 32, 78, 45, 117, 40, 82, 48, 78, -122, 48, 2, 78, 13, -113, -57, 98,
            17, 78, -20, 79, 26, 101, -80, 80, 90, 0, 67, 0, 111, 0, 109, 0, 109, 0, 97, 0, 110, 0, 100, 127, 22, 120, 1, 101, -71, 95, 15, -1, 12, 78,
            13, 79, 26, 117, 40, 78, 75, 82, 77, 118, -124, 0, 32, 0, 119, 0, 114, 0, 105, 0, 116, 0, 101, 0, 85, 0, 84, 0, 70, 0, 56];
        let buffer: &[i8] = unsafe {
            transmute(dst.as_slice())
        };
        assert_eq!(expect.as_slice(), buffer);
    }
}
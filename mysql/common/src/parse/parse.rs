use thiserror::Error;

pub struct NoEnoughData;
pub struct InvalidFloat;

#[derive(Debug, Clone, Copy, Error)]
pub enum CheckError {
    #[error("no enough data")]
    NoEnoughData,
}

macro_rules! impl_check {
    ($t:ty, $arr:ty => $($name:ident: $m:ident),*) => {
        $(
            fn $name(&mut self) -> Result<$t, CheckError> {
                let arr: $arr = self.read_array()?;
               Ok(<$t>::$m(arr))
            }
        )*
    };
}

pub trait InputBuf {
    fn left(&self) -> usize;
    fn slice(&self) -> &[u8];
    fn jump_to(&mut self, pos: usize) -> Result<(), CheckError>;

    fn read_vec(&mut self, count: usize) -> Result<Vec<u8>, CheckError> {
        if self.left() < count {
            return Err(CheckError::NoEnoughData);
        }
        let data: Vec<u8> = self.slice()[..count].to_vec();
        self.jump_to(count)?;
        Ok(data)
    }

    fn read_to_end(&mut self) -> Vec<u8> {
        let len = self.left();
        self.read_vec(len).unwrap()
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CheckError> {
        if self.left() < N {
            return Err(CheckError::NoEnoughData);
        }
        let mut arr: [u8; N] = [0; N];
        arr.copy_from_slice(&self.slice()[..N]);
        self.jump_to(N)?;
        Ok(arr)
    }

    impl_check!(u8, [u8; 1] => read_u8_be: from_be_bytes, read_u8_le: from_le_bytes, read_u8_ne: from_ne_bytes);
    impl_check!(u16, [u8; 2] => read_u16_be: from_be_bytes, read_u16_le: from_le_bytes, read_u16_ne: from_ne_bytes);
    impl_check!(u32, [u8; 4] => read_u32_be: from_be_bytes, read_u32_le: from_le_bytes, read_u32_ne: from_ne_bytes);
    impl_check!(u64, [u8; 8] => read_u64_be: from_be_bytes, read_u64_le: from_le_bytes, read_u64_ne: from_ne_bytes);
    impl_check!(u128, [u8; 16] => read_u128_be: from_be_bytes, read_u128_le: from_le_bytes, read_u128_ne: from_ne_bytes);
    impl_check!(i8, [u8; 1] => read_i8_be: from_be_bytes, read_i8_le: from_le_bytes, read_i8_ne: from_ne_bytes);
    impl_check!(i16, [u8; 2] => read_i16_be: from_be_bytes, read_i16_le: from_le_bytes, read_i16_ne: from_ne_bytes);
    impl_check!(i32, [u8; 4] => read_i32_be: from_be_bytes, read_i32_le: from_le_bytes, read_i32_ne: from_ne_bytes);
    impl_check!(i64, [u8; 8] => read_i64_be: from_be_bytes, read_i64_le: from_le_bytes, read_i64_ne: from_ne_bytes);
    impl_check!(i128, [u8; 16] => read_i128_be: from_be_bytes, read_i128_le: from_le_bytes, read_i128_ne: from_ne_bytes);
    impl_check!(f32, [u8; 4] => read_f32_be: from_be_bytes, read_f32_le: from_le_bytes, read_f32_ne: from_ne_bytes);
    impl_check!(f64, [u8; 8] => read_f64_be: from_be_bytes, read_f64_le: from_le_bytes, read_f64_ne: from_ne_bytes);
}

impl<T: InputBuf, U> InputBuf for (T, U) {
    fn left(&self) -> usize {
        self.0.left()
    }

    fn slice(&self) -> &[u8] {
        self.0.slice()
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CheckError> {
        self.0.read_array()
    }

    fn read_vec(&mut self, count: usize) -> Result<Vec<u8>, CheckError> {
        self.0.read_vec(count)
    }

    fn jump_to(&mut self, pos: usize) -> Result<(), CheckError> {
        self.0.jump_to(pos)
    }
}

impl<T: InputBuf, U, V> InputBuf for (T, U, V) {
    fn left(&self) -> usize {
        self.0.left()
    }

    fn slice(&self) -> &[u8] {
        self.0.slice()
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CheckError> {
        self.0.read_array()
    }

    fn jump_to(&mut self, pos: usize) -> Result<(), CheckError> {
        self.0.jump_to(pos)
    }
}

impl InputBuf for &[u8] {
    fn left(&self) -> usize {
        self.len()
    }

    fn slice(&self) -> &[u8] {
        self
    }

    fn jump_to(&mut self, pos: usize) -> Result<(), CheckError> {
        if self.len() < pos {
            return Err(CheckError::NoEnoughData);
        }
        *self = &self[pos..];
        Ok(())
    }
}

#[cfg(feature = "bytes")]
mod impl_bytes {
    use crate::{CheckError, InputBuf};
    use bytes::Buf;

    impl InputBuf for bytes::BytesMut {
        fn left(&self) -> usize {
            self.remaining()
        }

        fn slice(&self) -> &[u8] {
            self.chunk()
        }

        fn jump_to(&mut self, pos: usize) -> Result<(), CheckError> {
            if self.remaining() < pos {
                return Err(CheckError::NoEnoughData);
            }
            self.advance(pos);
            Ok(())
        }
    }

    impl InputBuf for bytes::Bytes {
        fn left(&self) -> usize {
            self.remaining()
        }

        fn slice(&self) -> &[u8] {
            self.chunk()
        }

        fn jump_to(&mut self, pos: usize) -> Result<(), CheckError> {
            if self.remaining() < pos {
                return Err(CheckError::NoEnoughData);
            }
            self.advance(pos);
            Ok(())
        }
    }
}
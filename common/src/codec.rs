use bytes::BytesMut;
use crate::err::DecodeError::{DecodeError, Needed};
use crate::parse::parse::{InputBuf};

/// Decode 结果集定义
pub type DecodeResult<T> = Result<T, DecodeError>;

pub trait Decode<I: InputBuf, Output = Self, Error = DecodeError>: Sized {
    fn decode(input: &mut I) -> Result<Output, Error>;
}

pub trait Encode {
    fn encode(&self, buf: &mut BytesMut);
}

impl From<Needed> for DecodeError {
    fn from(err: Needed) -> Self {
        Self::Incomplete(err)
    }
}

impl<I: InputBuf> Decode<I> for Vec<u8> {
    fn decode(input: &mut I) -> Result<Self, DecodeError> {
        Ok(input.read_to_end())
    }
}

macro_rules! from_prime {
    ($t:ty, $name:ident) => {
        impl From<$t> for $name {
            fn from(value: $t) -> Self {
                Self(value.to_le_bytes())
            }
        }
    };
    ($t:ty, $name:ident, $max:expr, $idx:expr) => {
        impl From<$t> for $name {
            fn from(value: $t) -> Self {
                assert!(value <= $max);
                let mut val = Self::default();
                val.0.copy_from_slice(&value.to_le_bytes()[..($idx + 1)]);
                val
            }
        }
    };
}

macro_rules! custom_impl {
    ($t:ty, $name:ident, $len:literal) => {
        impl $name {
            pub fn new(value: [u8; $len]) -> $name {
                Self(value)
            }

            pub fn int(&self) -> $t {
                let data: $t = 0;
                let mut data = data.to_le_bytes();
                let len = self.0.len();
                data[..len].copy_from_slice(&self.0);
                <$t>::from_le_bytes(data)
            }

            pub fn bytes(&self) -> &[u8] {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.int())
            }
        }

        impl<I: InputBuf> Decode<I> for $name where DecodeError: From<Needed> {
            fn decode(input: &mut I) -> Result<Self, DecodeError> {
                Ok(Self(input.read_array()?))
            }
        }

        impl Encode for $name {
            fn encode(&self, buf: &mut BytesMut) {
                buf.extend_from_slice(&self.0);
            }
        }
    };
}

macro_rules! fix {
    ($name:ident, $len:literal, $min_ty:ty, $max:expr) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $name(pub(crate) [u8; $len]);

        from_prime!($min_ty, $name, $max, $len - 1);
        custom_impl!($min_ty, $name, $len);
    };
    ($name:ident, $len:literal, $min_ty:ty) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $name(pub(crate) [u8; $len]);
        from_prime!($min_ty, $name);
        custom_impl!($min_ty, $name, $len);
    };
}

fix!(Int1, 1, u8);
fix!(Int2, 2, u16);
fix!(Int3, 3, u32, u32::MAX >> 1);
fix!(Int4, 4, u32);
fix!(Int6, 6, u64, u64::MAX >> 2);
fix!(Int8, 8, u64);

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
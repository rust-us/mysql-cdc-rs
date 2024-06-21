use std::fmt::{Debug, Formatter};

use bincode::{DefaultOptions, Options};
use tracing::error;

use common::err::CResult;
use common::err::decode_error::ReError;

use crate::codec::codec::Codec;

#[derive(Clone)]
pub struct BinaryCodec {
    serialize_options: DefaultOptions,
}

pub enum CodecStyle {
    // 小端定长
    LittleFix(u64),
    // 大端定长
    BigFix(u64),
    // 小端变长
    LittleVar,
    // 大端变长
    BigVar,
}

impl Codec for BinaryCodec {
    fn new() -> Self where Self: Sized {
        BinaryCodec {
            serialize_options: bincode::options(),
        }
    }

    fn name(&self) -> String {
        String::from("BinaryCodec")
    }
}

impl BinaryCodec {
    /// Serializes a serializable object into a `Vec` of bytes.
    pub fn binary_serialize<T: ?Sized>(&self, codec_style: &CodecStyle, value: &T) -> CResult<Vec<u8>>
        where
            T: serde::Serialize,
    {
        match codec_style {
            CodecStyle::LittleFix(limit) => {
                let bytes = self.serialize_options
                    .with_limit(*limit)
                    .with_little_endian()
                    .with_fixint_encoding()
                    .serialize(value).or_else(|e| {
                    error!("binary serialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(bytes)
            }
            CodecStyle::BigFix(limit) => {
                let bytes = self.serialize_options
                    .with_limit(*limit)
                    .with_big_endian()
                    .with_fixint_encoding()
                    .serialize(value).or_else(|e| {
                    error!("binary serialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(bytes)
            }
            CodecStyle::LittleVar => {
                let bytes = self.serialize_options
                    .allow_trailing_bytes()
                    .with_no_limit()
                    .with_little_endian()
                    .with_varint_encoding()
                    .serialize(value).or_else(|e| {
                    error!("binary serialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(bytes)
            }
            CodecStyle::BigVar => {
                let bytes = self.serialize_options
                    .allow_trailing_bytes()
                    .with_no_limit()
                    .with_big_endian()
                    .with_varint_encoding()
                    .serialize(value).or_else(|e| {
                    error!("binary serialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(bytes)
            }
        }
    }

    /// Deserializes a slice of bytes into an instance of `T`.
    pub fn binary_deserialize<'a, T>(&self, codec_style: &CodecStyle, bytes: &'a [u8]) -> CResult<T>
        where
            T: serde::de::Deserialize<'a>,
    {
        match codec_style {
            CodecStyle::LittleFix(limit) => {
                let r = self.serialize_options
                    .with_limit(*limit)
                    .with_little_endian()
                    .with_fixint_encoding()
                    .deserialize::<T>(bytes).or_else(|e| {
                    error!("binary deserialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(r)
            }
            CodecStyle::BigFix(limit) => {
                let r = self.serialize_options
                    .with_limit(*limit)
                    .with_big_endian()
                    .with_fixint_encoding()
                    .deserialize::<T>(bytes).or_else(|e| {
                    error!("binary deserialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(r)
            }
            CodecStyle::LittleVar => {
                let r = self.serialize_options
                    .allow_trailing_bytes()
                    .with_no_limit()
                    .with_little_endian()
                    .with_varint_encoding()
                    .deserialize::<T>(bytes).or_else(|e| {
                    error!("binary deserialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(r)
            }
            CodecStyle::BigVar => {
                let r = self.serialize_options
                    .allow_trailing_bytes()
                    .with_no_limit()
                    .with_big_endian()
                    .with_varint_encoding()
                    .deserialize::<T>(bytes).or_else(|e| {
                    error!("binary deserialize err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                Ok(r)
            }
        }
    }
}

impl Debug for BinaryCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryCodec")
            .field("name", &self.name())
            .field("serializer", &"bincode")
            .finish()
    }
}
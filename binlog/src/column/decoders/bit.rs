use std::io::Cursor;
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;
use crate::utils::read_bitmap_big_endian;

/// Decoder for BIT type
pub struct BitDecoder;

impl BitDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl TypeDecoder for BitDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        let length = (metadata.metadata >> 8) * 8 + (metadata.metadata & 0xFF);
        let bitmap = read_bitmap_big_endian(cursor, length as usize)?;
        Ok(ColumnValue::Bit(bitmap))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::Bit as u8
    }

    fn type_name(&self) -> &'static str {
        "BIT"
    }

    fn validate_length(&self, data: &[u8], metadata: &ColumnMetadata) -> Result<(), ReError> {
        let expected_bits = (metadata.metadata >> 8) * 8 + (metadata.metadata & 0xFF);
        let expected_bytes = (expected_bits + 7) / 8;
        
        if data.len() < expected_bytes as usize {
            return Err(ReError::String(format!(
                "BIT field data too short: expected {} bytes, got {}",
                expected_bytes, data.len()
            )));
        }
        
        Ok(())
    }

    fn expected_size(&self, metadata: &ColumnMetadata) -> Option<usize> {
        let bits = (metadata.metadata >> 8) * 8 + (metadata.metadata & 0xFF);
        Some(((bits + 7) / 8) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bit_decoder() {
        let decoder = BitDecoder::new();
        let data = vec![0b10101010u8]; // 8 bits: 10101010
        let mut cursor = Cursor::new(data.as_slice());
        // metadata: high byte = 0 (no extra bits), low byte = 8 (8 bits total)
        let metadata = ColumnMetadata::new(SrcColumnType::Bit as u8, 8);
        
        let result = decoder.decode(&mut cursor, &metadata).unwrap();
        if let ColumnValue::Bit(bits) = result {
            assert_eq!(bits.len(), 8);
            // Note: bits are reversed, so 10101010 becomes 01010101
            assert_eq!(bits, vec![false, true, false, true, false, true, false, true]);
        } else {
            panic!("Expected BIT value");
        }
    }

    #[test]
    fn test_bit_decoder_validation() {
        let decoder = BitDecoder::new();
        let data = vec![0u8]; // Only 1 byte
        let metadata = ColumnMetadata::new(SrcColumnType::Bit as u8, 16); // Expects 2 bytes
        
        let result = decoder.validate_length(&data, &metadata);
        assert!(result.is_err());
    }
}
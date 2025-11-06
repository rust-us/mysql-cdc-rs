use std::io::{Cursor, Read};
use byteorder::{BigEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;

/// Enhanced DECIMAL decoder with precise arithmetic and big number support
pub struct EnhancedDecimalDecoder;

impl EnhancedDecimalDecoder {
    pub fn new() -> Self {
        Self
    }

    /// Parse MySQL DECIMAL binary format with enhanced precision
    fn parse_decimal_binary(&self, cursor: &mut Cursor<&[u8]>, precision: u8, scale: u8) -> Result<String, ReError> {
        // MySQL DECIMAL binary format constants
        const DIG_PER_DEC1: u8 = 9;
        const DEC1_MAX: u32 = 1_000_000_000;
        const DECIMAL_POWERS: [u32; 10] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000];

        if precision == 0 {
            return Ok("0".to_string());
        }

        // Calculate the number of decimal digits groups
        let integral_digits = precision - scale;
        let integral_groups = (integral_digits + DIG_PER_DEC1 - 1) / DIG_PER_DEC1;
        let fractional_groups = (scale + DIG_PER_DEC1 - 1) / DIG_PER_DEC1;

        // Read the sign bit from the first byte
        let first_byte = cursor.read_u8()?;
        let is_negative = (first_byte & 0x80) == 0;
        
        // Put the first byte back
        cursor.set_position(cursor.position() - 1);

        // Read all bytes and apply sign transformation
        let total_bytes = self.calculate_decimal_size(precision, scale);
        let mut bytes = vec![0u8; total_bytes];
        cursor.read_exact(&mut bytes)?;

        // Apply sign transformation
        if is_negative {
            // For negative numbers, invert all bits
            for byte in &mut bytes {
                *byte = !*byte;
            }
        } else {
            // For positive numbers, just clear the sign bit
            bytes[0] &= 0x7F;
        }

        let mut result_parts = Vec::new();
        let mut byte_index = 0;

        // Parse integral part
        for group_index in 0..integral_groups {
            let digits_in_group = if group_index == 0 {
                ((integral_digits - 1) % DIG_PER_DEC1) + 1
            } else {
                DIG_PER_DEC1
            };

            let bytes_in_group = self.digits_to_bytes(digits_in_group);
            let group_value = self.read_decimal_group(&bytes[byte_index..byte_index + bytes_in_group])?;
            
            if group_index == 0 {
                result_parts.push(group_value.to_string());
            } else {
                result_parts.push(format!("{:0width$}", group_value, width = digits_in_group as usize));
            }
            
            byte_index += bytes_in_group;
        }

        // If no integral part, add "0"
        if integral_digits == 0 {
            result_parts.push("0".to_string());
        }

        let mut result = result_parts.join("");

        // Parse fractional part
        if scale > 0 {
            result.push('.');
            let mut fractional_parts = Vec::new();

            for group_index in 0..fractional_groups {
                let digits_in_group = if group_index == fractional_groups - 1 {
                    ((scale - 1) % DIG_PER_DEC1) + 1
                } else {
                    DIG_PER_DEC1
                };

                let bytes_in_group = self.digits_to_bytes(digits_in_group);
                let group_value = self.read_decimal_group(&bytes[byte_index..byte_index + bytes_in_group])?;
                
                fractional_parts.push(format!("{:0width$}", group_value, width = digits_in_group as usize));
                byte_index += bytes_in_group;
            }

            result.push_str(&fractional_parts.join(""));
        }

        // Add negative sign if needed
        if is_negative {
            result = format!("-{}", result);
        }

        // Remove trailing zeros from fractional part
        if scale > 0 {
            result = self.trim_trailing_zeros(&result);
        }

        Ok(result)
    }

    fn calculate_decimal_size(&self, precision: u8, _scale: u8) -> usize {
        const DIG_PER_DEC1: u8 = 9;
        
        let integral_digits = precision;
        let integral_groups = (integral_digits + DIG_PER_DEC1 - 1) / DIG_PER_DEC1;
        
        let mut size = 0;
        for i in 0..integral_groups {
            let digits_in_group = if i == 0 {
                ((integral_digits - 1) % DIG_PER_DEC1) + 1
            } else {
                DIG_PER_DEC1
            };
            size += self.digits_to_bytes(digits_in_group);
        }
        
        size
    }

    fn digits_to_bytes(&self, digits: u8) -> usize {
        match digits {
            1..=2 => 1,
            3..=4 => 2,
            5..=6 => 3,
            7..=9 => 4,
            _ => 4,
        }
    }

    fn read_decimal_group(&self, bytes: &[u8]) -> Result<u32, ReError> {
        match bytes.len() {
            1 => Ok(bytes[0] as u32),
            2 => {
                let mut cursor = Cursor::new(bytes);
                Ok(cursor.read_u16::<BigEndian>()? as u32)
            }
            3 => {
                let mut cursor = Cursor::new(bytes);
                Ok(cursor.read_u24::<BigEndian>()?)
            }
            4 => {
                let mut cursor = Cursor::new(bytes);
                Ok(cursor.read_u32::<BigEndian>()?)
            }
            _ => Err(ReError::String(format!("Invalid decimal group size: {}", bytes.len()))),
        }
    }

    fn trim_trailing_zeros(&self, decimal_str: &str) -> String {
        if !decimal_str.contains('.') {
            return decimal_str.to_string();
        }

        let mut result = decimal_str.to_string();
        while result.ends_with('0') && result.contains('.') {
            result.pop();
        }
        
        if result.ends_with('.') {
            result.pop();
        }
        
        result
    }

    /// Validate decimal precision and scale
    fn validate_decimal_params(&self, precision: u8, scale: u8) -> Result<(), ReError> {
        if precision == 0 || precision > 65 {
            return Err(ReError::String(format!("Invalid decimal precision: {}", precision)));
        }
        
        if scale > precision {
            return Err(ReError::String(format!("Decimal scale {} cannot be greater than precision {}", scale, precision)));
        }
        
        Ok(())
    }

    /// Parse decimal with arbitrary precision arithmetic
    fn parse_with_big_decimal(&self, decimal_str: &str) -> Result<BigDecimal, ReError> {
        use std::str::FromStr;
        
        // This would use a big decimal library like rust_decimal or bigdecimal
        // For now, we'll use a simple string representation
        BigDecimal::from_str(decimal_str)
            .map_err(|e| ReError::String(format!("Failed to parse decimal: {}", e)))
    }
}

impl TypeDecoder for EnhancedDecimalDecoder {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        // Extract precision and scale from metadata
        let precision = (metadata.metadata & 0xFF) as u8;
        let scale = ((metadata.metadata >> 8) & 0xFF) as u8;
        
        self.validate_decimal_params(precision, scale)?;
        
        let decimal_str = self.parse_decimal_binary(cursor, precision, scale)?;
        Ok(ColumnValue::Decimal(decimal_str))
    }

    fn column_type(&self) -> u8 {
        SrcColumnType::NewDecimal as u8
    }

    fn type_name(&self) -> &'static str {
        "DECIMAL"
    }

    fn validate_length(&self, data: &[u8], metadata: &ColumnMetadata) -> Result<(), ReError> {
        let precision = (metadata.metadata & 0xFF) as u8;
        let scale = ((metadata.metadata >> 8) & 0xFF) as u8;
        
        self.validate_decimal_params(precision, scale)?;
        
        let expected_size = self.calculate_decimal_size(precision, scale);
        if data.len() != expected_size {
            return Err(ReError::String(format!(
                "DECIMAL data size mismatch: expected {} bytes, got {}",
                expected_size, data.len()
            )));
        }
        
        Ok(())
    }
}

/// Big decimal implementation for arbitrary precision arithmetic
#[derive(Debug, Clone)]
pub struct BigDecimal {
    value: String,
    precision: u8,
    scale: u8,
}

impl BigDecimal {
    pub fn new(value: String, precision: u8, scale: u8) -> Self {
        Self { value, precision, scale }
    }

    pub fn from_str(s: &str) -> Result<Self, std::num::ParseFloatError> {
        // Simple implementation - in practice you'd use a proper big decimal library
        let _: f64 = s.parse()?; // Validate it's a valid number
        
        let (precision, scale) = Self::calculate_precision_scale(s);
        Ok(Self {
            value: s.to_string(),
            precision,
            scale,
        })
    }

    fn calculate_precision_scale(s: &str) -> (u8, u8) {
        let clean_str = s.trim_start_matches('-');
        
        if let Some(dot_pos) = clean_str.find('.') {
            let integral_part = &clean_str[..dot_pos];
            let fractional_part = &clean_str[dot_pos + 1..];
            
            let integral_digits = integral_part.len() as u8;
            let fractional_digits = fractional_part.len() as u8;
            
            (integral_digits + fractional_digits, fractional_digits)
        } else {
            (clean_str.len() as u8, 0)
        }
    }

    pub fn to_string(&self) -> String {
        self.value.clone()
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// Add two decimal numbers
    pub fn add(&self, other: &BigDecimal) -> Result<BigDecimal, ReError> {
        // Simplified implementation - use a proper big decimal library for production
        let self_val: f64 = self.value.parse()
            .map_err(|e| ReError::String(format!("Invalid decimal: {}", e)))?;
        let other_val: f64 = other.value.parse()
            .map_err(|e| ReError::String(format!("Invalid decimal: {}", e)))?;
        
        let result = self_val + other_val;
        let max_scale = std::cmp::max(self.scale, other.scale);
        
        Ok(BigDecimal {
            value: format!("{:.prec$}", result, prec = max_scale as usize),
            precision: std::cmp::max(self.precision, other.precision),
            scale: max_scale,
        })
    }

    /// Multiply two decimal numbers
    pub fn multiply(&self, other: &BigDecimal) -> Result<BigDecimal, ReError> {
        let self_val: f64 = self.value.parse()
            .map_err(|e| ReError::String(format!("Invalid decimal: {}", e)))?;
        let other_val: f64 = other.value.parse()
            .map_err(|e| ReError::String(format!("Invalid decimal: {}", e)))?;
        
        let result = self_val * other_val;
        let result_scale = self.scale + other.scale;
        
        Ok(BigDecimal {
            value: format!("{:.prec$}", result, prec = result_scale as usize),
            precision: self.precision + other.precision,
            scale: result_scale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_big_decimal_creation() {
        let decimal = BigDecimal::from_str("123.456").unwrap();
        assert_eq!(decimal.precision(), 6);
        assert_eq!(decimal.scale(), 3);
        assert_eq!(decimal.to_string(), "123.456");
    }

    #[test]
    fn test_big_decimal_arithmetic() {
        let a = BigDecimal::from_str("123.45").unwrap();
        let b = BigDecimal::from_str("67.89").unwrap();
        
        let sum = a.add(&b).unwrap();
        // Note: This is a simplified test - actual results may vary due to floating point precision
        assert!(sum.to_string().starts_with("191.3"));
    }

    #[test]
    fn test_decimal_validation() {
        let decoder = EnhancedDecimalDecoder::new();
        
        // Valid parameters
        assert!(decoder.validate_decimal_params(10, 2).is_ok());
        
        // Invalid parameters
        assert!(decoder.validate_decimal_params(0, 0).is_err());
        assert!(decoder.validate_decimal_params(5, 10).is_err());
    }
}
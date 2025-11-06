use std::collections::HashMap;
use common::err::decode_error::ReError;

/// Enhanced character set converter with support for multiple MySQL charsets
pub struct CharsetConverter {
    default_charset: String,
    charset_mappings: HashMap<u16, CharsetInfo>,
    conversion_cache: HashMap<(u16, Vec<u8>), String>,
}

/// Information about a character set
#[derive(Debug, Clone)]
pub struct CharsetInfo {
    pub id: u16,
    pub name: String,
    pub description: String,
    pub max_length: u8,
    pub is_multibyte: bool,
    pub encoding: CharsetEncoding,
}

/// Supported character set encodings
#[derive(Debug, Clone)]
pub enum CharsetEncoding {
    Utf8,
    Utf8mb4,
    Latin1,
    Ascii,
    Binary,
    Cp1252,
    Gbk,
    Big5,
    Sjis,
    Euckr,
    Custom(String),
}

impl CharsetConverter {
    pub fn new(default_charset: &str) -> Self {
        let mut converter = Self {
            default_charset: default_charset.to_string(),
            charset_mappings: HashMap::new(),
            conversion_cache: HashMap::new(),
        };
        
        converter.register_default_charsets();
        converter
    }

    /// Register default MySQL character sets
    fn register_default_charsets(&mut self) {
        // Common MySQL character sets
        self.register_charset(CharsetInfo {
            id: 1,
            name: "big5".to_string(),
            description: "Big5 Traditional Chinese".to_string(),
            max_length: 2,
            is_multibyte: true,
            encoding: CharsetEncoding::Big5,
        });

        self.register_charset(CharsetInfo {
            id: 3,
            name: "dec8".to_string(),
            description: "DEC West European".to_string(),
            max_length: 1,
            is_multibyte: false,
            encoding: CharsetEncoding::Latin1,
        });

        self.register_charset(CharsetInfo {
            id: 8,
            name: "latin1".to_string(),
            description: "cp1252 West European".to_string(),
            max_length: 1,
            is_multibyte: false,
            encoding: CharsetEncoding::Latin1,
        });

        self.register_charset(CharsetInfo {
            id: 11,
            name: "ascii".to_string(),
            description: "US ASCII".to_string(),
            max_length: 1,
            is_multibyte: false,
            encoding: CharsetEncoding::Ascii,
        });

        self.register_charset(CharsetInfo {
            id: 28,
            name: "gbk".to_string(),
            description: "GBK Simplified Chinese".to_string(),
            max_length: 2,
            is_multibyte: true,
            encoding: CharsetEncoding::Gbk,
        });

        self.register_charset(CharsetInfo {
            id: 33,
            name: "utf8".to_string(),
            description: "UTF-8 Unicode".to_string(),
            max_length: 3,
            is_multibyte: true,
            encoding: CharsetEncoding::Utf8,
        });

        self.register_charset(CharsetInfo {
            id: 45,
            name: "utf8mb4".to_string(),
            description: "UTF-8 Unicode".to_string(),
            max_length: 4,
            is_multibyte: true,
            encoding: CharsetEncoding::Utf8mb4,
        });

        self.register_charset(CharsetInfo {
            id: 63,
            name: "binary".to_string(),
            description: "Binary pseudo charset".to_string(),
            max_length: 1,
            is_multibyte: false,
            encoding: CharsetEncoding::Binary,
        });

        self.register_charset(CharsetInfo {
            id: 95,
            name: "cp932".to_string(),
            description: "SJIS for Windows Japanese".to_string(),
            max_length: 2,
            is_multibyte: true,
            encoding: CharsetEncoding::Sjis,
        });

        self.register_charset(CharsetInfo {
            id: 129,
            name: "euckr".to_string(),
            description: "EUC-KR Korean".to_string(),
            max_length: 2,
            is_multibyte: true,
            encoding: CharsetEncoding::Euckr,
        });
    }

    /// Register a character set
    pub fn register_charset(&mut self, charset_info: CharsetInfo) {
        self.charset_mappings.insert(charset_info.id, charset_info);
    }

    /// Convert string data from a specific charset to UTF-8
    pub fn convert_string(&mut self, data: &[u8], charset_id: Option<u16>) -> Result<String, ReError> {
        let charset_id = charset_id.unwrap_or(self.get_default_charset_id());
        
        // Check cache first
        let cache_key = (charset_id, data.to_vec());
        if let Some(cached_result) = self.conversion_cache.get(&cache_key) {
            return Ok(cached_result.clone());
        }

        let charset_info = self.charset_mappings.get(&charset_id)
            .ok_or_else(|| ReError::String(format!("Unknown charset ID: {}", charset_id)))?;

        let result = match &charset_info.encoding {
            CharsetEncoding::Utf8 | CharsetEncoding::Utf8mb4 => {
                self.convert_utf8(data)
            }
            CharsetEncoding::Latin1 => {
                self.convert_latin1(data)
            }
            CharsetEncoding::Ascii => {
                self.convert_ascii(data)
            }
            CharsetEncoding::Binary => {
                self.convert_binary(data)
            }
            CharsetEncoding::Gbk => {
                self.convert_gbk(data)
            }
            CharsetEncoding::Big5 => {
                self.convert_big5(data)
            }
            CharsetEncoding::Sjis => {
                self.convert_sjis(data)
            }
            CharsetEncoding::Euckr => {
                self.convert_euckr(data)
            }
            CharsetEncoding::Cp1252 => {
                self.convert_cp1252(data)
            }
            CharsetEncoding::Custom(name) => {
                Err(ReError::String(format!("Custom charset '{}' not implemented", name)))
            }
        };

        // Cache the result if successful
        if let Ok(ref converted) = result {
            self.conversion_cache.insert(cache_key, converted.clone());
        }

        result
    }

    /// Convert UTF-8 data
    fn convert_utf8(&self, data: &[u8]) -> Result<String, ReError> {
        String::from_utf8(data.to_vec())
            .map_err(|e| ReError::String(format!("UTF-8 conversion error: {}", e)))
    }

    /// Convert Latin-1 (ISO-8859-1) data
    fn convert_latin1(&self, data: &[u8]) -> Result<String, ReError> {
        // Latin-1 is a subset of Unicode, so we can directly convert
        let mut result = String::with_capacity(data.len());
        for &byte in data {
            result.push(byte as char);
        }
        Ok(result)
    }

    /// Convert ASCII data
    fn convert_ascii(&self, data: &[u8]) -> Result<String, ReError> {
        for &byte in data {
            if byte > 127 {
                return Err(ReError::String(format!("Invalid ASCII byte: {}", byte)));
            }
        }
        Ok(String::from_utf8_lossy(data).to_string())
    }

    /// Convert binary data (no conversion, return as hex string)
    fn convert_binary(&self, data: &[u8]) -> Result<String, ReError> {
        Ok(hex::encode(data))
    }

    /// Convert GBK data (simplified implementation)
    fn convert_gbk(&self, data: &[u8]) -> Result<String, ReError> {
        // This is a simplified implementation
        // In a production system, you would use a proper GBK decoder
        self.convert_multibyte_fallback(data, "GBK")
    }

    /// Convert Big5 data (simplified implementation)
    fn convert_big5(&self, data: &[u8]) -> Result<String, ReError> {
        // This is a simplified implementation
        // In a production system, you would use a proper Big5 decoder
        self.convert_multibyte_fallback(data, "Big5")
    }

    /// Convert Shift-JIS data (simplified implementation)
    fn convert_sjis(&self, data: &[u8]) -> Result<String, ReError> {
        // This is a simplified implementation
        // In a production system, you would use a proper Shift-JIS decoder
        self.convert_multibyte_fallback(data, "Shift-JIS")
    }

    /// Convert EUC-KR data (simplified implementation)
    fn convert_euckr(&self, data: &[u8]) -> Result<String, ReError> {
        // This is a simplified implementation
        // In a production system, you would use a proper EUC-KR decoder
        self.convert_multibyte_fallback(data, "EUC-KR")
    }

    /// Convert CP1252 data
    fn convert_cp1252(&self, data: &[u8]) -> Result<String, ReError> {
        // CP1252 is similar to Latin-1 but with different characters in the 128-159 range
        let mut result = String::with_capacity(data.len());
        for &byte in data {
            let ch = match byte {
                0x80 => '€',
                0x82 => '‚',
                0x83 => 'ƒ',
                0x84 => '„',
                0x85 => '…',
                0x86 => '†',
                0x87 => '‡',
                0x88 => 'ˆ',
                0x89 => '‰',
                0x8A => 'Š',
                0x8B => '‹',
                0x8C => 'Œ',
                0x8E => 'Ž',
                0x91 => '\'',
                0x92 => '\'',
                0x93 => '"',
                0x94 => '"',
                0x95 => '•',
                0x96 => '–',
                0x97 => '—',
                0x98 => '˜',
                0x99 => '™',
                0x9A => 'š',
                0x9B => '›',
                0x9C => 'œ',
                0x9E => 'ž',
                0x9F => 'Ÿ',
                _ => byte as char,
            };
            result.push(ch);
        }
        Ok(result)
    }

    /// Fallback for multibyte character sets (returns UTF-8 lossy conversion)
    fn convert_multibyte_fallback(&self, data: &[u8], charset_name: &str) -> Result<String, ReError> {
        // Try UTF-8 first
        if let Ok(utf8_str) = String::from_utf8(data.to_vec()) {
            return Ok(utf8_str);
        }
        
        // Fallback to lossy conversion
        let result = String::from_utf8_lossy(data).to_string();
        
        // Log a warning about the fallback
        tracing::warn!("Using lossy conversion for {} charset", charset_name);
        
        Ok(result)
    }

    /// Get charset information by ID
    pub fn get_charset_info(&self, charset_id: u16) -> Option<&CharsetInfo> {
        self.charset_mappings.get(&charset_id)
    }

    /// Get charset ID by name
    pub fn get_charset_id_by_name(&self, name: &str) -> Option<u16> {
        self.charset_mappings.iter()
            .find(|(_, info)| info.name == name)
            .map(|(id, _)| *id)
    }

    /// Get default charset
    pub fn get_default_charset(&self) -> &str {
        &self.default_charset
    }

    /// Get default charset ID
    pub fn get_default_charset_id(&self) -> u16 {
        self.get_charset_id_by_name(&self.default_charset).unwrap_or(33) // UTF-8
    }

    /// Set default charset
    pub fn set_default_charset(&mut self, charset: String) {
        self.default_charset = charset;
    }

    /// Clear conversion cache
    pub fn clear_cache(&mut self) {
        self.conversion_cache.clear();
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> CharsetCacheStats {
        CharsetCacheStats {
            cache_size: self.conversion_cache.len(),
            registered_charsets: self.charset_mappings.len(),
        }
    }

    /// Detect charset from byte order mark (BOM) or content analysis
    pub fn detect_charset(&self, data: &[u8]) -> Option<u16> {
        if data.len() < 3 {
            return None;
        }

        // Check for UTF-8 BOM
        if data.starts_with(&[0xEF, 0xBB, 0xBF]) {
            return Some(33); // UTF-8
        }

        // Check for UTF-16 BOM
        if data.starts_with(&[0xFF, 0xFE]) || data.starts_with(&[0xFE, 0xFF]) {
            return Some(33); // UTF-8 (closest match)
        }

        // If all bytes are < 128, assume ASCII
        if data.iter().all(|&b| b < 128) {
            return Some(11); // ASCII
        }

        // Simple heuristic: if all bytes are valid UTF-8, assume UTF-8
        if String::from_utf8(data.to_vec()).is_ok() {
            return Some(33); // UTF-8
        }

        None
    }

    /// Validate that data is valid for the given charset
    pub fn validate_charset_data(&self, data: &[u8], charset_id: u16) -> Result<(), ReError> {
        let charset_info = self.charset_mappings.get(&charset_id)
            .ok_or_else(|| ReError::String(format!("Unknown charset ID: {}", charset_id)))?;

        match &charset_info.encoding {
            CharsetEncoding::Ascii => {
                for &byte in data {
                    if byte > 127 {
                        return Err(ReError::String(format!("Invalid ASCII byte: {}", byte)));
                    }
                }
            }
            CharsetEncoding::Utf8 | CharsetEncoding::Utf8mb4 => {
                String::from_utf8(data.to_vec())
                    .map_err(|e| ReError::String(format!("Invalid UTF-8: {}", e)))?;
            }
            _ => {
                // For other charsets, we don't have strict validation
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CharsetCacheStats {
    pub cache_size: usize,
    pub registered_charsets: usize,
}

/// Utility functions for character set handling
pub struct CharsetUtils;

impl CharsetUtils {
    /// Check if a charset is multibyte
    pub fn is_multibyte_charset(charset_id: u16) -> bool {
        matches!(charset_id, 1 | 28 | 33 | 45 | 95 | 129) // Big5, GBK, UTF-8, UTF-8MB4, CP932, EUC-KR
    }

    /// Get the maximum character length for a charset
    pub fn get_max_char_length(charset_id: u16) -> u8 {
        match charset_id {
            1 | 28 | 95 | 129 => 2, // Big5, GBK, CP932, EUC-KR
            33 => 3,                 // UTF-8
            45 => 4,                 // UTF-8MB4
            _ => 1,                  // Single-byte charsets
        }
    }

    /// Convert charset name to standard form
    pub fn normalize_charset_name(name: &str) -> String {
        name.to_lowercase().replace("-", "").replace("_", "")
    }

    /// Check if two charset names refer to the same charset
    pub fn charset_names_equal(name1: &str, name2: &str) -> bool {
        Self::normalize_charset_name(name1) == Self::normalize_charset_name(name2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_charset_converter_utf8() {
        let mut converter = CharsetConverter::new("utf8mb4");
        let data = "Hello, 世界!".as_bytes();
        let result = converter.convert_string(data, Some(33)).unwrap();
        assert_eq!(result, "Hello, 世界!");
    }

    #[test]
    fn test_charset_converter_latin1() {
        let mut converter = CharsetConverter::new("utf8mb4");
        let data = b"Hello, caf\xe9!"; // "café" in Latin-1
        let result = converter.convert_string(data, Some(8)).unwrap();
        assert_eq!(result, "Hello, café!");
    }

    #[test]
    fn test_charset_converter_ascii() {
        let mut converter = CharsetConverter::new("utf8mb4");
        let data = b"Hello, World!";
        let result = converter.convert_string(data, Some(11)).unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_charset_detection() {
        let converter = CharsetConverter::new("utf8mb4");
        
        // UTF-8 BOM
        let utf8_bom = &[0xEF, 0xBB, 0xBF, b'H', b'e', b'l', b'l', b'o'];
        assert_eq!(converter.detect_charset(utf8_bom), Some(33));
        
        // ASCII data
        let ascii_data = b"Hello, World!";
        assert_eq!(converter.detect_charset(ascii_data), Some(11));
    }

    #[test]
    fn test_charset_utils() {
        assert!(CharsetUtils::is_multibyte_charset(33)); // UTF-8
        assert!(!CharsetUtils::is_multibyte_charset(8)); // Latin-1
        
        assert_eq!(CharsetUtils::get_max_char_length(45), 4); // UTF-8MB4
        assert_eq!(CharsetUtils::get_max_char_length(8), 1);  // Latin-1
        
        assert!(CharsetUtils::charset_names_equal("utf-8", "utf8"));
        assert!(CharsetUtils::charset_names_equal("UTF_8", "utf8"));
    }
}
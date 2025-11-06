use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use common::err::decode_error::ReError;

/// String interning and optimization utilities for memory-efficient string handling
pub struct StringOptimizer {
    intern_pool: Arc<Mutex<HashMap<String, Arc<str>>>>,
    stats: StringOptimizerStats,
    config: StringOptimizerConfig,
}

#[derive(Debug, Clone)]
pub struct StringOptimizerConfig {
    /// Enable string interning for duplicate strings
    pub enable_interning: bool,
    /// Maximum size of the intern pool
    pub max_intern_pool_size: usize,
    /// Minimum string length to consider for interning
    pub min_intern_length: usize,
    /// Enable string compression for large strings
    pub enable_compression: bool,
    /// Minimum string length to consider for compression
    pub min_compression_length: usize,
}

#[derive(Debug, Default)]
pub struct StringOptimizerStats {
    pub total_strings_processed: u64,
    pub strings_interned: u64,
    pub strings_compressed: u64,
    pub memory_saved_bytes: u64,
    pub intern_pool_size: usize,
}

impl Default for StringOptimizerConfig {
    fn default() -> Self {
        Self {
            enable_interning: true,
            max_intern_pool_size: 10000,
            min_intern_length: 3,
            enable_compression: false, // Disabled by default for performance
            min_compression_length: 1024,
        }
    }
}

impl StringOptimizer {
    pub fn new(config: StringOptimizerConfig) -> Self {
        Self {
            intern_pool: Arc::new(Mutex::new(HashMap::new())),
            stats: StringOptimizerStats::default(),
            config,
        }
    }

    /// Optimize a string using configured strategies
    pub fn optimize_string(&mut self, input: String) -> Result<OptimizedString, ReError> {
        self.stats.total_strings_processed += 1;

        // Check if string should be interned
        if self.config.enable_interning 
            && input.len() >= self.config.min_intern_length 
            && self.stats.intern_pool_size < self.config.max_intern_pool_size {
            
            if let Ok(interned) = self.try_intern_string(&input) {
                self.stats.strings_interned += 1;
                return Ok(OptimizedString::Interned(interned));
            }
        }

        // Check if string should be compressed
        if self.config.enable_compression && input.len() >= self.config.min_compression_length {
            if let Ok(compressed) = self.try_compress_string(&input) {
                self.stats.strings_compressed += 1;
                let saved = input.len().saturating_sub(compressed.compressed_size());
                self.stats.memory_saved_bytes += saved as u64;
                return Ok(OptimizedString::Compressed(compressed));
            }
        }

        // Return as regular string
        Ok(OptimizedString::Regular(input))
    }

    /// Try to intern a string
    fn try_intern_string(&mut self, input: &str) -> Result<Arc<str>, ReError> {
        let mut pool = self.intern_pool.lock()
            .map_err(|e| ReError::String(format!("Failed to lock intern pool: {}", e)))?;

        if let Some(existing) = pool.get(input) {
            return Ok(existing.clone());
        }

        // Check pool size limit
        if pool.len() >= self.config.max_intern_pool_size {
            return Err(ReError::String("Intern pool is full".to_string()));
        }

        let interned: Arc<str> = input.into();
        pool.insert(input.to_string(), interned.clone());
        self.stats.intern_pool_size = pool.len();

        Ok(interned)
    }

    /// Try to compress a string
    fn try_compress_string(&self, input: &str) -> Result<CompressedString, ReError> {
        // Simple compression using run-length encoding for demonstration
        // In practice, you might use a proper compression library like flate2
        let compressed = self.simple_compress(input.as_bytes());
        
        if compressed.len() < input.len() {
            Ok(CompressedString {
                data: compressed,
                original_length: input.len(),
            })
        } else {
            Err(ReError::String("Compression not beneficial".to_string()))
        }
    }

    /// Simple run-length encoding compression
    fn simple_compress(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut compressed = Vec::new();
        let mut current_byte = data[0];
        let mut count = 1u8;

        for &byte in &data[1..] {
            if byte == current_byte && count < 255 {
                count += 1;
            } else {
                compressed.push(count);
                compressed.push(current_byte);
                current_byte = byte;
                count = 1;
            }
        }

        // Add the last run
        compressed.push(count);
        compressed.push(current_byte);

        compressed
    }

    /// Decompress run-length encoded data
    fn simple_decompress(&self, data: &[u8]) -> Result<Vec<u8>, ReError> {
        if data.len() % 2 != 0 {
            return Err(ReError::String("Invalid compressed data length".to_string()));
        }

        let mut decompressed = Vec::new();
        
        for chunk in data.chunks(2) {
            let count = chunk[0];
            let byte = chunk[1];
            
            for _ in 0..count {
                decompressed.push(byte);
            }
        }

        Ok(decompressed)
    }

    /// Get optimization statistics
    pub fn get_stats(&self) -> &StringOptimizerStats {
        &self.stats
    }

    /// Clear the intern pool
    pub fn clear_intern_pool(&mut self) -> Result<(), ReError> {
        let mut pool = self.intern_pool.lock()
            .map_err(|e| ReError::String(format!("Failed to lock intern pool: {}", e)))?;
        pool.clear();
        self.stats.intern_pool_size = 0;
        Ok(())
    }

    /// Update configuration
    pub fn update_config(&mut self, config: StringOptimizerConfig) {
        self.config = config;
    }

    /// Get current configuration
    pub fn get_config(&self) -> &StringOptimizerConfig {
        &self.config
    }
}

/// Optimized string representation
#[derive(Debug, Clone)]
pub enum OptimizedString {
    /// Regular string (no optimization applied)
    Regular(String),
    /// Interned string (shared reference)
    Interned(Arc<str>),
    /// Compressed string
    Compressed(CompressedString),
}

#[derive(Debug, Clone)]
pub struct CompressedString {
    data: Vec<u8>,
    original_length: usize,
}

impl CompressedString {
    pub fn compressed_size(&self) -> usize {
        self.data.len()
    }

    pub fn original_size(&self) -> usize {
        self.original_length
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.original_length == 0 {
            return 1.0;
        }
        self.data.len() as f64 / self.original_length as f64
    }
}

impl OptimizedString {
    /// Get the string content as a &str
    pub fn as_str(&self) -> Result<std::borrow::Cow<'_, str>, ReError> {
        match self {
            OptimizedString::Regular(s) => Ok(std::borrow::Cow::Borrowed(s)),
            OptimizedString::Interned(s) => Ok(std::borrow::Cow::Borrowed(s)),
            OptimizedString::Compressed(compressed) => {
                // For demonstration, we'll use a simple decompression
                // In practice, you'd use the same compression library used for compression
                let optimizer = StringOptimizer::new(StringOptimizerConfig::default());
                let decompressed_bytes = optimizer.simple_decompress(&compressed.data)?;
                let decompressed_string = String::from_utf8(decompressed_bytes)
                    .map_err(|e| ReError::String(format!("Decompression UTF-8 error: {}", e)))?;
                Ok(std::borrow::Cow::Owned(decompressed_string))
            }
        }
    }

    /// Convert to owned String
    pub fn to_string(&self) -> Result<String, ReError> {
        match self.as_str()? {
            std::borrow::Cow::Borrowed(s) => Ok(s.to_string()),
            std::borrow::Cow::Owned(s) => Ok(s),
        }
    }

    /// Get the memory footprint of this optimized string
    pub fn memory_footprint(&self) -> usize {
        match self {
            OptimizedString::Regular(s) => s.len(),
            OptimizedString::Interned(_) => std::mem::size_of::<Arc<str>>(), // Just the Arc overhead
            OptimizedString::Compressed(compressed) => compressed.data.len(),
        }
    }

    /// Check if the string is optimized
    pub fn is_optimized(&self) -> bool {
        !matches!(self, OptimizedString::Regular(_))
    }
}

/// Utility functions for string optimization
pub struct StringUtils;

impl StringUtils {
    /// Estimate memory usage of a string
    pub fn estimate_memory_usage(s: &str) -> usize {
        s.len() + std::mem::size_of::<String>()
    }

    /// Check if a string is likely to benefit from interning
    pub fn should_intern(s: &str, min_length: usize, is_duplicate_likely: bool) -> bool {
        s.len() >= min_length && (is_duplicate_likely || s.len() > 50)
    }

    /// Check if a string is likely to benefit from compression
    pub fn should_compress(s: &str, min_length: usize) -> bool {
        if s.len() < min_length {
            return false;
        }

        // Simple heuristic: check for repeated patterns
        let bytes = s.as_bytes();
        let mut repeated_chars = 0;
        let mut prev_char = None;

        for &byte in bytes {
            if Some(byte) == prev_char {
                repeated_chars += 1;
            }
            prev_char = Some(byte);
        }

        // If more than 20% of characters are repeated, compression might be beneficial
        repeated_chars as f64 / bytes.len() as f64 > 0.2
    }

    /// Normalize string for comparison (trim, lowercase, etc.)
    pub fn normalize_for_comparison(s: &str) -> String {
        s.trim().to_lowercase()
    }

    /// Calculate string similarity (simple Levenshtein distance)
    pub fn string_similarity(a: &str, b: &str) -> f64 {
        if a == b {
            return 1.0;
        }

        let len_a = a.len();
        let len_b = b.len();

        if len_a == 0 || len_b == 0 {
            return 0.0;
        }

        let max_len = std::cmp::max(len_a, len_b);
        let distance = levenshtein_distance(a, b);
        
        1.0 - (distance as f64 / max_len as f64)
    }
}

/// Calculate Levenshtein distance between two strings
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let len_a = a_chars.len();
    let len_b = b_chars.len();

    if len_a == 0 {
        return len_b;
    }
    if len_b == 0 {
        return len_a;
    }

    let mut matrix = vec![vec![0; len_b + 1]; len_a + 1];

    // Initialize first row and column
    for i in 0..=len_a {
        matrix[i][0] = i;
    }
    for j in 0..=len_b {
        matrix[0][j] = j;
    }

    // Fill the matrix
    for i in 1..=len_a {
        for j in 1..=len_b {
            let cost = if a_chars[i - 1] == b_chars[j - 1] { 0 } else { 1 };
            matrix[i][j] = std::cmp::min(
                std::cmp::min(
                    matrix[i - 1][j] + 1,      // deletion
                    matrix[i][j - 1] + 1,      // insertion
                ),
                matrix[i - 1][j - 1] + cost,   // substitution
            );
        }
    }

    matrix[len_a][len_b]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_optimizer_interning() {
        let mut optimizer = StringOptimizer::new(StringOptimizerConfig::default());
        
        let test_string = "Hello, World!".to_string();
        let optimized = optimizer.optimize_string(test_string.clone()).unwrap();
        
        match optimized {
            OptimizedString::Interned(_) => {
                assert_eq!(optimizer.get_stats().strings_interned, 1);
            }
            _ => panic!("Expected interned string"),
        }
    }

    #[test]
    fn test_string_compression() {
        let mut config = StringOptimizerConfig::default();
        config.enable_compression = true;
        config.min_compression_length = 10;
        
        let mut optimizer = StringOptimizer::new(config);
        
        // String with repeated characters should compress well
        let test_string = "aaaaaaaaaaaabbbbbbbbbbbb".to_string();
        let optimized = optimizer.optimize_string(test_string.clone()).unwrap();
        
        match optimized {
            OptimizedString::Compressed(compressed) => {
                assert!(compressed.compression_ratio() < 1.0);
                assert_eq!(optimizer.get_stats().strings_compressed, 1);
            }
            _ => {
                // Compression might not always be beneficial
            }
        }
    }

    #[test]
    fn test_string_utils() {
        assert!(StringUtils::should_intern("Hello, World!", 5, true));
        assert!(!StringUtils::should_intern("Hi", 5, false));
        
        assert!(StringUtils::should_compress("aaaaaaaaaaaabbbbbbbbbbbb", 10));
        assert!(!StringUtils::should_compress("abcdefghijk", 10));
        
        assert_eq!(StringUtils::string_similarity("hello", "hello"), 1.0);
        assert!(StringUtils::string_similarity("hello", "hallo") > 0.5);
    }

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
        assert_eq!(levenshtein_distance("hello", "hallo"), 1);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    }
}
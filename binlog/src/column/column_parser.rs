
use std::io::Cursor;
use std::sync::Arc;
use std::collections::HashMap;
use common::err::decode_error::ReError;
use common::binlog::column::column_type::SrcColumnType;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::{TypeDecoder, TypeDecoderRegistry, ConflictResolutionStrategy};
use crate::column::custom_decoder::{CustomTypeRegistry, CustomTypeDecoder, CustomTypeInfo};
use crate::column::charset::CharsetConverter;
use crate::column::performance::{ColumnParsingPerformanceOptimizer, PerformanceConfig, TypeOptimizations, PerformanceMonitor};

// Legacy parsing functions are now in the legacy module
// pub use crate::column::legacy;

/// Main column parser that uses the new TypeDecoder system
pub struct ColumnParser {
    type_registry: TypeDecoderRegistry,
    custom_registry: CustomTypeRegistry,
    charset_converter: Option<CharsetConverter>,
    stats: ColumnParserStats,
    performance_optimizer: Option<ColumnParsingPerformanceOptimizer>,
}

/// Statistics for column parsing performance
#[derive(Debug, Default)]
pub struct ColumnParserStats {
    pub total_parsed: u64,
    pub parse_errors: u64,
    pub type_distribution: HashMap<u8, u64>,
    pub avg_parse_time_ns: u64,
}

// CharsetConverter is now imported from the charset module

impl ColumnParser {
    /// Create a new ColumnParser with default type decoders
    pub fn new() -> Self {
        let mut parser = Self {
            type_registry: TypeDecoderRegistry::new(),
            custom_registry: CustomTypeRegistry::new(),
            charset_converter: Some(CharsetConverter::new("utf8mb4")),
            stats: ColumnParserStats::default(),
            performance_optimizer: Some(ColumnParsingPerformanceOptimizer::new(PerformanceConfig::default())),
        };
        
        // Register default type decoders
        parser.register_default_decoders();
        parser
    }

    /// Create a new ColumnParser with a specific conflict resolution strategy
    pub fn with_conflict_strategy(strategy: ConflictResolutionStrategy) -> Self {
        let mut parser = Self {
            type_registry: TypeDecoderRegistry::with_conflict_strategy(strategy),
            custom_registry: CustomTypeRegistry::new(),
            charset_converter: Some(CharsetConverter::new("utf8mb4")),
            stats: ColumnParserStats::default(),
            performance_optimizer: Some(ColumnParsingPerformanceOptimizer::new(PerformanceConfig::default())),
        };
        
        // Register default type decoders
        parser.register_default_decoders();
        parser
    }

    /// Create a new ColumnParser with custom performance configuration
    pub fn with_performance_config(perf_config: PerformanceConfig) -> Self {
        let mut parser = Self {
            type_registry: TypeDecoderRegistry::new(),
            custom_registry: CustomTypeRegistry::new(),
            charset_converter: Some(CharsetConverter::new("utf8mb4")),
            stats: ColumnParserStats::default(),
            performance_optimizer: Some(ColumnParsingPerformanceOptimizer::new(perf_config)),
        };
        
        // Register default type decoders
        parser.register_default_decoders();
        parser
    }

    /// Parse a column value from binary data
    pub fn parse_column(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        metadata: &ColumnMetadata,
    ) -> Result<ColumnValue, ReError> {
        let start_time = std::time::Instant::now();
        let mut performance_monitor = PerformanceMonitor::new();
        let mut used_fast_path = false;

        // Check cache first if performance optimizer is enabled
        if let Some(ref optimizer) = self.performance_optimizer {
            let remaining_data = &cursor.get_ref()[cursor.position() as usize..];
            if let Some(cached_result) = optimizer.get_cached_result(metadata.column_type, metadata.metadata, remaining_data) {
                let parse_time = start_time.elapsed();
                optimizer.record_parse_stats(metadata.column_type, parse_time, true);
                self.update_stats(metadata.column_type, start_time, true);
                return Ok(cached_result);
            }
        }

        performance_monitor.checkpoint("cache_check");

        // Check if we can use fast path optimization
        let remaining_data = &cursor.get_ref()[cursor.position() as usize..];
        if TypeOptimizations::can_use_fast_path(metadata.column_type, remaining_data.len()) {
            if let Ok(fast_result) = self.try_fast_path_parse(cursor, metadata) {
                used_fast_path = true;
                let parse_time = start_time.elapsed();
                
                // Cache the result if beneficial
                if let Some(ref optimizer) = self.performance_optimizer {
                    optimizer.cache_result(metadata.column_type, metadata.metadata, remaining_data, fast_result.clone(), parse_time);
                    optimizer.record_parse_stats(metadata.column_type, parse_time, used_fast_path);
                }
                
                self.update_stats(metadata.column_type, start_time, true);
                return Ok(fast_result);
            }
        }

        performance_monitor.checkpoint("fast_path_check");

        // Get the appropriate decoder
        let decoder = self.type_registry
            .get_decoder(metadata.column_type)
            .ok_or_else(|| ReError::String(format!(
                "No decoder found for column type: {}",
                metadata.column_type
            )))?;

        performance_monitor.checkpoint("decoder_lookup");

        // Decode the value
        let result = decoder.decode(cursor, metadata);
        
        performance_monitor.checkpoint("decode");

        let parse_time = start_time.elapsed();
        
        // Cache successful results if performance optimizer is enabled
        if let (Ok(ref value), Some(ref optimizer)) = (&result, &self.performance_optimizer) {
            optimizer.cache_result(metadata.column_type, metadata.metadata, remaining_data, value.clone(), parse_time);
            optimizer.record_parse_stats(metadata.column_type, parse_time, used_fast_path);
        }
        
        // Update statistics
        self.update_stats(metadata.column_type, start_time, result.is_ok());
        
        result
    }

    /// Try to parse using fast path optimizations for simple types
    fn try_fast_path_parse(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        use byteorder::{LittleEndian, ReadBytesExt};
        
        match metadata.column_type {
            1 => { // TINYINT
                let value = cursor.read_u8()?;
                if metadata.unsigned {
                    Ok(ColumnValue::UTinyInt(value))
                } else {
                    Ok(ColumnValue::TinyInt(value as i8))
                }
            }
            2 => { // SMALLINT
                let value = cursor.read_u16::<LittleEndian>()?;
                if metadata.unsigned {
                    Ok(ColumnValue::USmallInt(value))
                } else {
                    Ok(ColumnValue::SmallInt(value as i16))
                }
            }
            3 => { // INT
                let value = cursor.read_u32::<LittleEndian>()?;
                if metadata.unsigned {
                    Ok(ColumnValue::UInt(value))
                } else {
                    Ok(ColumnValue::Int(value as i32))
                }
            }
            8 => { // BIGINT
                let value = cursor.read_u64::<LittleEndian>()?;
                if metadata.unsigned {
                    Ok(ColumnValue::UBigInt(value))
                } else {
                    Ok(ColumnValue::BigInt(value as i64))
                }
            }
            4 => { // FLOAT
                let value = cursor.read_f32::<LittleEndian>()?;
                Ok(ColumnValue::Float(value))
            }
            5 => { // DOUBLE
                let value = cursor.read_f64::<LittleEndian>()?;
                Ok(ColumnValue::Double(value))
            }
            13 => { // YEAR
                let year = 1900 + cursor.read_u8()? as u16;
                Ok(ColumnValue::Year(year))
            }
            _ => Err(ReError::String("Fast path not available for this type".to_string()))
        }
    }

    /// Register a type decoder
    pub fn register_type_decoder(
        &mut self,
        decoder: Arc<dyn TypeDecoder>,
        priority: i32,
        name: String,
    ) -> Result<(), ReError> {
        self.type_registry.register_decoder(decoder, priority, name)
    }

    /// Register a custom type decoder
    pub fn register_custom_type_decoder(
        &mut self,
        decoder: Arc<dyn TypeDecoder>,
        priority: i32,
        name: String,
    ) -> Result<(), ReError> {
        self.type_registry.register_custom_decoder(decoder, priority, name)
    }

    /// Register a custom type with its information
    pub fn register_custom_type(&mut self, type_info: CustomTypeInfo) -> Result<(), ReError> {
        // Register the type mapping in the type registry
        self.type_registry.register_custom_type_mapping(
            type_info.type_name.clone(),
            type_info.type_id,
        )?;
        
        // Register the type in the custom registry
        self.custom_registry.register_custom_type(type_info)
    }

    /// Register a custom decoder implementation
    pub fn register_custom_decoder_impl(&mut self, decoder: Box<dyn CustomTypeDecoder>) -> Result<(), ReError> {
        let supported_types = decoder.supported_types();
        
        // Register the decoder in the custom registry
        self.custom_registry.register_custom_decoder(decoder)?;
        
        // For each supported type, create an adapter and register it in the type registry
        for type_name in supported_types {
            if let Some(type_info) = self.custom_registry.get_type_info(&type_name) {
                if let Some(custom_decoder) = self.custom_registry.get_decoder(&type_name) {
                    // Create a new decoder instance for the adapter
                    // Note: This is a simplified approach. In practice, you'd need a way to clone the decoder
                    // or use Arc<dyn CustomTypeDecoder>
                }
            }
        }
        
        Ok(())
    }

    /// Check if a column type is supported
    pub fn supports_type(&self, column_type: u8) -> bool {
        self.type_registry.supports_type(column_type)
    }

    /// Get parsing statistics
    pub fn get_stats(&self) -> &ColumnParserStats {
        &self.stats
    }

    /// Reset parsing statistics
    pub fn reset_stats(&mut self) {
        self.stats = ColumnParserStats::default();
    }

    /// Set charset converter
    pub fn set_charset_converter(&mut self, converter: CharsetConverter) {
        self.charset_converter = Some(converter);
    }

    /// Get charset converter
    pub fn get_charset_converter(&self) -> Option<&CharsetConverter> {
        self.charset_converter.as_ref()
    }

    /// Set conflict resolution strategy
    pub fn set_conflict_strategy(&mut self, strategy: ConflictResolutionStrategy) {
        self.type_registry.set_conflict_strategy(strategy);
    }

    /// Get conflict resolution strategy
    pub fn get_conflict_strategy(&self) -> &ConflictResolutionStrategy {
        self.type_registry.get_conflict_strategy()
    }

    /// List all registered decoders
    pub fn list_decoders(&self) -> Vec<crate::column::type_decoder::DecoderInfo> {
        self.type_registry.list_decoders()
    }

    /// Get type registry statistics
    pub fn get_type_registry_stats(&self) -> crate::column::type_decoder::TypeDecoderStats {
        self.type_registry.get_stats()
    }

    /// Get custom type registry statistics
    pub fn get_custom_type_stats(&self) -> crate::column::custom_decoder::CustomTypeStats {
        self.custom_registry.get_stats()
    }

    /// Check if custom decoders are registered
    pub fn has_custom_decoders(&self) -> bool {
        self.type_registry.has_custom_decoders() || self.custom_registry.get_stats().total_decoders > 0
    }

    /// Unregister a decoder by name
    pub fn unregister_decoder(&mut self, name: &str) -> Result<(), ReError> {
        self.type_registry.unregister_decoder(name)
    }

    /// Add a registration callback
    pub fn add_registration_callback<F>(&mut self, callback: F) 
    where 
        F: Fn(&str, u8, i32) + Send + Sync + 'static 
    {
        self.type_registry.add_registration_callback(callback);
    }

    /// Enable or disable performance optimization
    pub fn set_performance_optimization(&mut self, enabled: bool) {
        if enabled && self.performance_optimizer.is_none() {
            self.performance_optimizer = Some(ColumnParsingPerformanceOptimizer::new(PerformanceConfig::default()));
        } else if !enabled {
            self.performance_optimizer = None;
        }
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> Option<crate::column::performance::PerformanceStats> {
        self.performance_optimizer.as_ref()?.get_stats()
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> Option<crate::column::performance::CacheStats> {
        self.performance_optimizer.as_ref()?.get_cache_stats()
    }

    /// Clear performance cache and reset statistics
    pub fn reset_performance_stats(&self) {
        if let Some(ref optimizer) = self.performance_optimizer {
            optimizer.reset();
        }
    }

    /// Optimize performance cache
    pub fn optimize_performance_cache(&self) {
        if let Some(ref optimizer) = self.performance_optimizer {
            optimizer.optimize_cache();
        }
    }

    /// Update performance configuration
    pub fn update_performance_config(&mut self, config: PerformanceConfig) {
        self.performance_optimizer = Some(ColumnParsingPerformanceOptimizer::new(config));
    }

    /// Register all default type decoders
    fn register_default_decoders(&mut self) {
        // Import and register all the built-in decoders
        use crate::column::decoders::*;
        
        // Numeric types
        let _ = self.register_type_decoder(
            Arc::new(TinyIntDecoder::new()),
            100,
            "builtin_tinyint".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(SmallIntDecoder::new()),
            100,
            "builtin_smallint".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(MediumIntDecoder::new()),
            100,
            "builtin_mediumint".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(IntDecoder::new()),
            100,
            "builtin_int".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(BigIntDecoder::new()),
            100,
            "builtin_bigint".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(FloatDecoder::new()),
            100,
            "builtin_float".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(DoubleDecoder::new()),
            100,
            "builtin_double".to_string(),
        );
        
        // String types
        let _ = self.register_type_decoder(
            Arc::new(VarCharDecoder::new()),
            100,
            "builtin_varchar".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(StringDecoder::new()),
            100,
            "builtin_string".to_string(),
        );
        
        // Date/time types
        let _ = self.register_type_decoder(
            Arc::new(DateDecoder::new()),
            100,
            "builtin_date".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(TimeDecoder::new()),
            100,
            "builtin_time".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(Time2Decoder::new()),
            100,
            "builtin_time2".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(DateTimeDecoder::new()),
            100,
            "builtin_datetime".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(DateTime2Decoder::new()),
            100,
            "builtin_datetime2".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(TimestampDecoder::new()),
            100,
            "builtin_timestamp".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(Timestamp2Decoder::new()),
            100,
            "builtin_timestamp2".to_string(),
        );
        
        // Blob and binary types
        let _ = self.register_type_decoder(
            Arc::new(BlobDecoder::new()),
            100,
            "builtin_blob".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(TinyBlobDecoder::new()),
            100,
            "builtin_tinyblob".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(MediumBlobDecoder::new()),
            100,
            "builtin_mediumblob".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(LongBlobDecoder::new()),
            100,
            "builtin_longblob".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(GeometryTypeDecoder::new()),
            100,
            "builtin_geometry".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(JsonTypeDecoder::new()),
            100,
            "builtin_json".to_string(),
        );
        
        // Other types
        let _ = self.register_type_decoder(
            Arc::new(BitDecoder::new()),
            100,
            "builtin_bit".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(YearDecoder::new()),
            100,
            "builtin_year".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(EnumDecoder::new()),
            100,
            "builtin_enum".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(SetDecoder::new()),
            100,
            "builtin_set".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(VarStringDecoder::new()),
            100,
            "builtin_varstring".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(DecimalDecoder::new()),
            90,
            "builtin_decimal".to_string(),
        );
        
        // Enhanced decoders with higher priority
        let _ = self.register_type_decoder(
            Arc::new(EnhancedDecimalDecoder::new()),
            100,
            "enhanced_decimal".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(JsonTypeDecoder::new()),
            100,
            "enhanced_json".to_string(),
        );
        
        let _ = self.register_type_decoder(
            Arc::new(GeometryTypeDecoder::new()),
            100,
            "enhanced_geometry".to_string(),
        );
    }

    fn update_stats(&mut self, column_type: u8, start_time: std::time::Instant, success: bool) {
        self.stats.total_parsed += 1;
        if !success {
            self.stats.parse_errors += 1;
        }
        
        *self.stats.type_distribution.entry(column_type).or_insert(0) += 1;
        
        let elapsed = start_time.elapsed().as_nanos() as u64;
        // Simple moving average for parse time
        self.stats.avg_parse_time_ns = 
            (self.stats.avg_parse_time_ns + elapsed) / 2;
    }
}

// CharsetConverter implementation is now in the charset module

impl Default for ColumnParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_parser_creation() {
        let parser = ColumnParser::new();
        assert!(parser.supports_type(SrcColumnType::Tiny as u8));
        assert!(parser.supports_type(SrcColumnType::VarChar as u8));
    }

    #[test]
    fn test_charset_converter() {
        let mut converter = CharsetConverter::new("utf8mb4");
        assert_eq!(converter.get_default_charset(), "utf8mb4");
        
        let test_data = "Hello, 世界!".as_bytes();
        let result = converter.convert_string(test_data, None).unwrap();
        assert_eq!(result, "Hello, 世界!");
    }
}

use std::io::Cursor;
use common::err::decode_error::ReError;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;

/// Trait for decoding specific MySQL column types
pub trait TypeDecoder: Send + Sync {
    /// Decode binary data into a ColumnValue
    fn decode(&self, cursor: &mut Cursor<&[u8]>, metadata: &ColumnMetadata) -> Result<ColumnValue, ReError>;
    
    /// Get the MySQL column type this decoder handles
    fn column_type(&self) -> u8;
    
    /// Get a human-readable name for this decoder
    fn type_name(&self) -> &'static str;
    
    /// Validate that the data length is appropriate for this type
    fn validate_length(&self, data: &[u8], metadata: &ColumnMetadata) -> Result<(), ReError> {
        // Default implementation - no validation
        Ok(())
    }
    
    /// Get the expected size in bytes for this type (if fixed size)
    fn expected_size(&self, metadata: &ColumnMetadata) -> Option<usize> {
        None
    }
}

/// Registry for type decoders with priority and conflict handling
pub struct TypeDecoderRegistry {
    decoders: std::collections::HashMap<u8, Vec<DecoderEntry>>,
    custom_type_mappings: std::collections::HashMap<String, u8>,
    conflict_resolution_strategy: ConflictResolutionStrategy,
    registration_callbacks: Vec<Box<dyn Fn(&str, u8, i32) + Send + Sync>>,
}

#[derive(Clone)]
struct DecoderEntry {
    decoder: std::sync::Arc<dyn TypeDecoder>,
    priority: i32,
    name: String,
    is_custom: bool,
    registration_time: std::time::SystemTime,
}

/// Strategy for resolving conflicts when multiple decoders are registered for the same type
#[derive(Debug, Clone)]
pub enum ConflictResolutionStrategy {
    /// Use the decoder with the highest priority
    HighestPriority,
    /// Use the most recently registered decoder
    MostRecent,
    /// Use the first registered decoder (ignore subsequent registrations)
    FirstWins,
    /// Fail registration if a conflict occurs
    FailOnConflict,
}

impl TypeDecoderRegistry {
    pub fn new() -> Self {
        Self {
            decoders: std::collections::HashMap::new(),
            custom_type_mappings: std::collections::HashMap::new(),
            conflict_resolution_strategy: ConflictResolutionStrategy::HighestPriority,
            registration_callbacks: Vec::new(),
        }
    }

    /// Create a new registry with a specific conflict resolution strategy
    pub fn with_conflict_strategy(strategy: ConflictResolutionStrategy) -> Self {
        Self {
            decoders: std::collections::HashMap::new(),
            custom_type_mappings: std::collections::HashMap::new(),
            conflict_resolution_strategy: strategy,
            registration_callbacks: Vec::new(),
        }
    }

    /// Register a type decoder with a given priority (higher priority = preferred)
    pub fn register_decoder(&mut self, decoder: std::sync::Arc<dyn TypeDecoder>, priority: i32, name: String) -> Result<(), ReError> {
        self.register_decoder_internal(decoder, priority, name, false)
    }

    /// Register a custom type decoder
    pub fn register_custom_decoder(&mut self, decoder: std::sync::Arc<dyn TypeDecoder>, priority: i32, name: String) -> Result<(), ReError> {
        self.register_decoder_internal(decoder, priority, name, true)
    }

    /// Register a custom type mapping (type name -> type ID)
    pub fn register_custom_type_mapping(&mut self, type_name: String, type_id: u8) -> Result<(), ReError> {
        if self.custom_type_mappings.contains_key(&type_name) {
            return Err(ReError::String(format!(
                "Custom type '{}' is already registered",
                type_name
            )));
        }
        
        self.custom_type_mappings.insert(type_name, type_id);
        Ok(())
    }

    /// Get type ID for a custom type name
    pub fn get_custom_type_id(&self, type_name: &str) -> Option<u8> {
        self.custom_type_mappings.get(type_name).copied()
    }

    /// Add a callback that gets called when a decoder is registered
    pub fn add_registration_callback<F>(&mut self, callback: F) 
    where 
        F: Fn(&str, u8, i32) + Send + Sync + 'static 
    {
        self.registration_callbacks.push(Box::new(callback));
    }

    /// Set the conflict resolution strategy
    pub fn set_conflict_strategy(&mut self, strategy: ConflictResolutionStrategy) {
        self.conflict_resolution_strategy = strategy.clone();
        
        // Re-sort all decoder lists according to the new strategy
        let strategy_for_sort = strategy;
        for entries in self.decoders.values_mut() {
            Self::sort_decoders_with_strategy(entries, &strategy_for_sort);
        }
    }

    fn register_decoder_internal(&mut self, decoder: std::sync::Arc<dyn TypeDecoder>, priority: i32, name: String, is_custom: bool) -> Result<(), ReError> {
        let column_type = decoder.column_type();
        
        // Check for conflicts based on strategy
        if let Some(existing_entries) = self.decoders.get(&column_type) {
            match self.conflict_resolution_strategy {
                ConflictResolutionStrategy::FailOnConflict => {
                    return Err(ReError::String(format!(
                        "Decoder for type {} already exists: {}",
                        column_type,
                        existing_entries[0].name
                    )));
                }
                ConflictResolutionStrategy::FirstWins => {
                    // Don't register if one already exists
                    return Ok(());
                }
                _ => {
                    // Other strategies allow multiple registrations
                }
            }
        }
        
        let entry = DecoderEntry {
            decoder,
            priority,
            name: name.clone(),
            is_custom,
            registration_time: std::time::SystemTime::now(),
        };
        
        let entries = self.decoders
            .entry(column_type)
            .or_insert_with(Vec::new);
        
        entries.push(entry);
        Self::sort_decoders_with_strategy(entries, &self.conflict_resolution_strategy);
        
        // Call registration callbacks
        for callback in &self.registration_callbacks {
            callback(&name, column_type, priority);
        }
        
        Ok(())
    }

    fn sort_decoders_with_strategy(entries: &mut Vec<DecoderEntry>, strategy: &ConflictResolutionStrategy) {
        match strategy {
            ConflictResolutionStrategy::HighestPriority => {
                entries.sort_by(|a, b| b.priority.cmp(&a.priority));
            }
            ConflictResolutionStrategy::MostRecent => {
                entries.sort_by(|a, b| b.registration_time.cmp(&a.registration_time));
            }
            ConflictResolutionStrategy::FirstWins => {
                entries.sort_by(|a, b| a.registration_time.cmp(&b.registration_time));
            }
            ConflictResolutionStrategy::FailOnConflict => {
                // Already handled in registration
                entries.sort_by(|a, b| b.priority.cmp(&a.priority));
            }
        }
    }

    /// Get the best decoder for a column type
    pub fn get_decoder(&self, column_type: u8) -> Option<std::sync::Arc<dyn TypeDecoder>> {
        self.decoders
            .get(&column_type)?
            .first()
            .map(|entry| entry.decoder.clone())
    }

    /// Get all decoders for a column type (sorted by priority)
    pub fn get_all_decoders(&self, column_type: u8) -> Vec<std::sync::Arc<dyn TypeDecoder>> {
        self.decoders
            .get(&column_type)
            .map(|entries| entries.iter().map(|e| e.decoder.clone()).collect())
            .unwrap_or_default()
    }

    /// Check if a decoder is registered for the given column type
    pub fn supports_type(&self, column_type: u8) -> bool {
        self.decoders.contains_key(&column_type)
    }

    /// Unregister a decoder by name
    pub fn unregister_decoder(&mut self, name: &str) -> Result<(), ReError> {
        for entries in self.decoders.values_mut() {
            if let Some(pos) = entries.iter().position(|e| e.name == name) {
                entries.remove(pos);
                return Ok(());
            }
        }
        
        Err(ReError::String(format!("Decoder '{}' not found", name)))
    }

    /// List all registered decoders
    pub fn list_decoders(&self) -> Vec<DecoderInfo> {
        let mut result = Vec::new();
        
        for (column_type, entries) in &self.decoders {
            for entry in entries {
                result.push(DecoderInfo {
                    name: entry.name.clone(),
                    column_type: *column_type,
                    priority: entry.priority,
                    is_custom: entry.is_custom,
                    type_name: entry.decoder.type_name().to_string(),
                });
            }
        }
        
        result.sort_by(|a, b| a.column_type.cmp(&b.column_type).then(b.priority.cmp(&a.priority)));
        result
    }

    /// Get statistics about registered decoders
    pub fn get_stats(&self) -> TypeDecoderStats {
        let total_types = self.decoders.len();
        let total_decoders = self.decoders.values().map(|v| v.len()).sum();
        let conflicts = self.decoders.values().filter(|v| v.len() > 1).count();
        let custom_decoders = self.decoders.values()
            .flatten()
            .filter(|e| e.is_custom)
            .count();
        let custom_type_mappings = self.custom_type_mappings.len();
        
        TypeDecoderStats {
            total_types,
            total_decoders,
            conflicts,
            custom_decoders,
            custom_type_mappings,
        }
    }

    /// Check if the registry has any custom decoders
    pub fn has_custom_decoders(&self) -> bool {
        self.decoders.values()
            .flatten()
            .any(|e| e.is_custom)
    }

    /// Get conflict resolution strategy
    pub fn get_conflict_strategy(&self) -> &ConflictResolutionStrategy {
        &self.conflict_resolution_strategy
    }
}

#[derive(Debug)]
pub struct TypeDecoderStats {
    pub total_types: usize,
    pub total_decoders: usize,
    pub conflicts: usize,
    pub custom_decoders: usize,
    pub custom_type_mappings: usize,
}

#[derive(Debug, Clone)]
pub struct DecoderInfo {
    pub name: String,
    pub column_type: u8,
    pub priority: i32,
    pub is_custom: bool,
    pub type_name: String,
}

impl Default for TypeDecoderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    
    struct MockDecoder {
        column_type: u8,
        type_name: &'static str,
    }
    
    impl TypeDecoder for MockDecoder {
        fn decode(&self, _cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
            Ok(ColumnValue::Null)
        }
        
        fn column_type(&self) -> u8 {
            self.column_type
        }
        
        fn type_name(&self) -> &'static str {
            self.type_name
        }
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = TypeDecoderRegistry::new();
        let decoder = Arc::new(MockDecoder { column_type: 1, type_name: "TINYINT" });
        
        registry.register_decoder(decoder.clone(), 10, "test_decoder".to_string()).unwrap();
        
        let retrieved = registry.get_decoder(1).unwrap();
        assert_eq!(retrieved.type_name(), "TINYINT");
        assert!(registry.supports_type(1));
        assert!(!registry.supports_type(2));
    }

    #[test]
    fn test_registry_priority() {
        let mut registry = TypeDecoderRegistry::new();
        let decoder1 = Arc::new(MockDecoder { column_type: 1, type_name: "DECODER1" });
        let decoder2 = Arc::new(MockDecoder { column_type: 1, type_name: "DECODER2" });
        
        registry.register_decoder(decoder1, 5, "decoder1".to_string()).unwrap();
        registry.register_decoder(decoder2, 10, "decoder2".to_string()).unwrap();
        
        let retrieved = registry.get_decoder(1).unwrap();
        assert_eq!(retrieved.type_name(), "DECODER2"); // Higher priority
    }
}
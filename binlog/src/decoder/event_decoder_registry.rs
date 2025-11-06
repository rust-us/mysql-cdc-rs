use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::io::Cursor;
use common::err::decode_error::{ReError, ErrorContext};
use crate::b_type::LogEventType;
use crate::events::binlog_event::BinlogEvent;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::decoder::table_cache_manager::TableCacheManager;

/// Priority levels for event decoders
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DecoderPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Trait for event decoders that can parse specific event types
pub trait EventDecoder: Send + Sync + std::fmt::Debug {
    /// Get the event type this decoder handles
    fn event_type(&self) -> u8;
    
    /// Get the priority of this decoder
    fn priority(&self) -> DecoderPriority {
        DecoderPriority::Normal
    }
    
    /// Get the name of this decoder for debugging
    fn name(&self) -> &'static str;
    
    /// Decode the event from the given data
    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, crate::events::protocol::table_map_event::TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError>;
    
    /// Validate that this decoder can handle the given data
    fn can_decode(&self, event_type: u8, data: &[u8]) -> bool {
        self.event_type() == event_type && !data.is_empty()
    }
    
    /// Get decoder version for compatibility checking
    fn version(&self) -> u32 {
        1
    }
}

/// Registry entry for an event decoder
#[derive(Debug)]
struct DecoderEntry {
    decoder: Box<dyn EventDecoder>,
    priority: DecoderPriority,
    name: String,
    version: u32,
}

/// Registry for managing event decoders
#[derive(Debug)]
pub struct EventDecoderRegistry {
    decoders: RwLock<HashMap<u8, Vec<DecoderEntry>>>,
    default_decoder: Option<Box<dyn EventDecoder>>,
}

impl EventDecoderRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            decoders: RwLock::new(HashMap::new()),
            default_decoder: None,
        }
    }

    /// Register a new event decoder
    pub fn register_decoder(&self, decoder: Box<dyn EventDecoder>) -> Result<(), ReError> {
        let event_type = decoder.event_type();
        let priority = decoder.priority();
        let name = decoder.name().to_string();
        let version = decoder.version();

        let mut decoders = self.decoders.write().map_err(|_| {
            ReError::Error("Failed to acquire write lock on decoders".to_string())
        })?;

        let entry = DecoderEntry {
            decoder,
            priority,
            name: name.clone(),
            version,
        };

        // Check for conflicts
        if let Some(existing_decoders) = decoders.get(&event_type) {
            for existing in existing_decoders {
                if existing.priority == priority && existing.name == name {
                    return Err(ReError::Error(format!(
                        "Decoder conflict: {} already registered for event type 0x{:02x} with priority {:?}",
                        name, event_type, priority
                    )));
                }
            }
        }

        // Add the decoder to the list for this event type
        decoders.entry(event_type).or_insert_with(Vec::new).push(entry);

        // Sort by priority (highest first)
        if let Some(decoder_list) = decoders.get_mut(&event_type) {
            decoder_list.sort_by(|a, b| b.priority.cmp(&a.priority));
        }

        Ok(())
    }

    /// Unregister a decoder by name and event type
    pub fn unregister_decoder(&self, event_type: u8, name: &str) -> Result<bool, ReError> {
        let mut decoders = self.decoders.write().map_err(|_| {
            ReError::Error("Failed to acquire write lock on decoders".to_string())
        })?;

        if let Some(decoder_list) = decoders.get_mut(&event_type) {
            let original_len = decoder_list.len();
            decoder_list.retain(|entry| entry.name != name);
            Ok(decoder_list.len() != original_len)
        } else {
            Ok(false)
        }
    }

    /// Get the best decoder for the given event type and data
    pub fn get_decoder(&self, event_type: u8, data: &[u8]) -> Result<Option<&dyn EventDecoder>, ReError> {
        let decoders = self.decoders.read().map_err(|_| {
            ReError::Error("Failed to acquire read lock on decoders".to_string())
        })?;

        if let Some(decoder_list) = decoders.get(&event_type) {
            // Find the first decoder that can handle this data (they're sorted by priority)
            for entry in decoder_list {
                if entry.decoder.can_decode(event_type, data) {
                    // This is unsafe but necessary due to lifetime constraints
                    // We ensure safety by keeping the registry alive during parsing
                    let decoder_ptr = entry.decoder.as_ref() as *const dyn EventDecoder;
                    return Ok(Some(unsafe { &*decoder_ptr }));
                }
            }
        }

        Ok(None)
    }

    /// Check if a decoder is registered for the given event type
    pub fn has_decoder(&self, event_type: u8) -> bool {
        if let Ok(decoders) = self.decoders.read() {
            decoders.contains_key(&event_type) && !decoders[&event_type].is_empty()
        } else {
            false
        }
    }

    /// Get all registered event types
    pub fn get_supported_event_types(&self) -> Vec<u8> {
        if let Ok(decoders) = self.decoders.read() {
            decoders.keys().copied().collect()
        } else {
            Vec::new()
        }
    }

    /// Get decoder information for debugging
    pub fn get_decoder_info(&self, event_type: u8) -> Vec<(String, DecoderPriority, u32)> {
        if let Ok(decoders) = self.decoders.read() {
            if let Some(decoder_list) = decoders.get(&event_type) {
                return decoder_list.iter()
                    .map(|entry| (entry.name.clone(), entry.priority, entry.version))
                    .collect();
            }
        }
        Vec::new()
    }

    /// Clear all registered decoders
    pub fn clear(&self) -> Result<(), ReError> {
        let mut decoders = self.decoders.write().map_err(|_| {
            ReError::Error("Failed to acquire write lock on decoders".to_string())
        })?;
        decoders.clear();
        Ok(())
    }

    /// Get the number of registered decoders
    pub fn decoder_count(&self) -> usize {
        if let Ok(decoders) = self.decoders.read() {
            decoders.values().map(|list| list.len()).sum()
        } else {
            0
        }
    }

    /// Decode an event using the appropriate registered decoder
    pub fn decode_event(
        &self,
        event_type: u8,
        data: &[u8],
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, crate::events::protocol::table_map_event::TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let error_context = ErrorContext::new()
            .with_event_type(event_type)
            .with_position(header.borrow().get_log_pos())
            .with_operation("decode_event".to_string());

        // Get the appropriate decoder
        let decoder = self.get_decoder(event_type, data)?
            .ok_or_else(|| ReError::unsupported_event_type(event_type, error_context.clone()))?;

        // Create cursor for parsing
        let mut cursor = Cursor::new(data);

        // Decode the event
        decoder.decode(&mut cursor, header, context, table_map, table_cache_manager)
            .map_err(|e| ReError::event_parse_error_with_source(
                format!("Failed to decode event type 0x{:02x} using decoder {}", 
                    event_type, decoder.name()),
                error_context,
                e
            ))
    }
}

impl Default for EventDecoderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Thread-safe wrapper for the registry
pub type EventDecoderRegistryRef = Arc<EventDecoderRegistry>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Mock decoder for testing
    #[derive(Debug)]
    struct MockDecoder {
        event_type: u8,
        name: &'static str,
        priority: DecoderPriority,
    }

    impl EventDecoder for MockDecoder {
        fn event_type(&self) -> u8 {
            self.event_type
        }

        fn priority(&self) -> DecoderPriority {
            self.priority
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn decode(
            &self,
            _cursor: &mut Cursor<&[u8]>,
            _header: HeaderRef,
            _context: LogContextRef,
            _table_map: Option<&HashMap<u64, crate::events::protocol::table_map_event::TableMapEvent>>,
            _table_cache_manager: Option<&TableCacheManager>,
        ) -> Result<BinlogEvent, ReError> {
            // Return a mock event for testing
            Ok(BinlogEvent::IgnorableLogEvent)
        }
    }

    #[test]
    fn test_registry_basic_operations() {
        let registry = EventDecoderRegistry::new();

        // Test registration
        let decoder = Box::new(MockDecoder {
            event_type: 1,
            name: "test_decoder",
            priority: DecoderPriority::Normal,
        });

        assert!(registry.register_decoder(decoder).is_ok());
        assert!(registry.has_decoder(1));
        assert!(!registry.has_decoder(2));

        // Test unregistration
        assert!(registry.unregister_decoder(1, "test_decoder").unwrap());
        assert!(!registry.has_decoder(1));
    }

    #[test]
    fn test_priority_ordering() {
        let registry = EventDecoderRegistry::new();

        // Register decoders with different priorities
        let low_decoder = Box::new(MockDecoder {
            event_type: 1,
            name: "low_priority",
            priority: DecoderPriority::Low,
        });

        let high_decoder = Box::new(MockDecoder {
            event_type: 1,
            name: "high_priority",
            priority: DecoderPriority::High,
        });

        registry.register_decoder(low_decoder).unwrap();
        registry.register_decoder(high_decoder).unwrap();

        // The high priority decoder should be returned first
        let decoder_info = registry.get_decoder_info(1);
        assert_eq!(decoder_info.len(), 2);
        assert_eq!(decoder_info[0].0, "high_priority");
        assert_eq!(decoder_info[1].0, "low_priority");
    }
}
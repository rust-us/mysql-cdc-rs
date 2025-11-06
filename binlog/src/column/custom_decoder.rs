use std::io::Cursor;
use std::collections::HashMap;
use common::err::decode_error::ReError;
use crate::column::column_metadata::ColumnMetadata;
use crate::column::column_value_unified::ColumnValue;
use crate::column::type_decoder::TypeDecoder;

/// Trait for custom data type decoders that can be dynamically registered
pub trait CustomTypeDecoder: Send + Sync {
    /// Decode custom type data
    fn decode_custom_type(&self, data: &[u8], type_info: &CustomTypeInfo) -> Result<ColumnValue, ReError>;
    
    /// Get the list of custom type names this decoder supports
    fn supported_types(&self) -> Vec<String>;
    
    /// Get decoder metadata
    fn decoder_info(&self) -> CustomDecoderInfo;
    
    /// Validate custom type data before decoding
    fn validate_custom_data(&self, data: &[u8], type_info: &CustomTypeInfo) -> Result<(), ReError> {
        // Default implementation - no validation
        Ok(())
    }
}

/// Information about a custom type
#[derive(Debug, Clone)]
pub struct CustomTypeInfo {
    pub type_name: String,
    pub type_id: u8,
    pub metadata: HashMap<String, String>,
    pub version: u32,
}

/// Information about a custom decoder
#[derive(Debug, Clone)]
pub struct CustomDecoderInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: Option<String>,
}

/// Adapter to make CustomTypeDecoder work with the TypeDecoder trait
pub struct CustomTypeDecoderAdapter {
    custom_decoder: Box<dyn CustomTypeDecoder>,
    type_info: CustomTypeInfo,
}

impl CustomTypeDecoderAdapter {
    pub fn new(custom_decoder: Box<dyn CustomTypeDecoder>, type_info: CustomTypeInfo) -> Self {
        Self {
            custom_decoder,
            type_info,
        }
    }
}

impl TypeDecoder for CustomTypeDecoderAdapter {
    fn decode(&self, cursor: &mut Cursor<&[u8]>, _metadata: &ColumnMetadata) -> Result<ColumnValue, ReError> {
        // Read all remaining data
        let mut data = Vec::new();
        std::io::Read::read_to_end(cursor, &mut data)
            .map_err(|e| ReError::IoError(e))?;
        
        self.custom_decoder.decode_custom_type(&data, &self.type_info)
    }

    fn column_type(&self) -> u8 {
        self.type_info.type_id
    }

    fn type_name(&self) -> &'static str {
        // This is a limitation - we need to return a static str
        // In practice, custom types would need to be registered with known names
        "CUSTOM"
    }

    fn validate_length(&self, data: &[u8], _metadata: &ColumnMetadata) -> Result<(), ReError> {
        self.custom_decoder.validate_custom_data(data, &self.type_info)
    }
}

/// Example custom decoder for a hypothetical "Point2D" type
pub struct Point2DDecoder;

impl CustomTypeDecoder for Point2DDecoder {
    fn decode_custom_type(&self, data: &[u8], _type_info: &CustomTypeInfo) -> Result<ColumnValue, ReError> {
        if data.len() != 16 {
            return Err(ReError::String(format!(
                "Point2D requires exactly 16 bytes, got {}",
                data.len()
            )));
        }

        // Decode two f64 values (x, y coordinates)
        let x = f64::from_le_bytes([
            data[0], data[1], data[2], data[3],
            data[4], data[5], data[6], data[7],
        ]);
        let y = f64::from_le_bytes([
            data[8], data[9], data[10], data[11],
            data[12], data[13], data[14], data[15],
        ]);

        let mut metadata = HashMap::new();
        metadata.insert("x".to_string(), x.to_string());
        metadata.insert("y".to_string(), y.to_string());

        Ok(ColumnValue::Custom {
            type_name: "Point2D".to_string(),
            data: data.to_vec(),
            metadata,
        })
    }

    fn supported_types(&self) -> Vec<String> {
        vec!["Point2D".to_string()]
    }

    fn decoder_info(&self) -> CustomDecoderInfo {
        CustomDecoderInfo {
            name: "Point2D Decoder".to_string(),
            version: "1.0.0".to_string(),
            description: "Decoder for 2D point coordinates (x, y as f64)".to_string(),
            author: Some("Binlog Parser Core".to_string()),
        }
    }

    fn validate_custom_data(&self, data: &[u8], _type_info: &CustomTypeInfo) -> Result<(), ReError> {
        if data.len() != 16 {
            return Err(ReError::String(format!(
                "Point2D requires exactly 16 bytes, got {}",
                data.len()
            )));
        }
        Ok(())
    }
}

/// Registry for custom type decoders
pub struct CustomTypeRegistry {
    decoders: HashMap<String, Box<dyn CustomTypeDecoder>>,
    type_mappings: HashMap<String, CustomTypeInfo>,
}

impl CustomTypeRegistry {
    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
            type_mappings: HashMap::new(),
        }
    }

    /// Register a custom type decoder
    pub fn register_custom_decoder(&mut self, decoder: Box<dyn CustomTypeDecoder>) -> Result<(), ReError> {
        let supported_types = decoder.supported_types();
        let decoder_info = decoder.decoder_info();

        for type_name in &supported_types {
            if self.decoders.contains_key(type_name) {
                return Err(ReError::String(format!(
                    "Custom decoder for type '{}' already registered",
                    type_name
                )));
            }
        }

        for type_name in supported_types {
            self.decoders.insert(type_name.clone(), Box::new(DummyDecoder));
        }

        // Store the actual decoder under its primary name (first supported type)
        if let Some(primary_type) = decoder.supported_types().first() {
            self.decoders.insert(primary_type.clone(), decoder);
        }

        Ok(())
    }

    /// Register a custom type mapping
    pub fn register_custom_type(&mut self, type_info: CustomTypeInfo) -> Result<(), ReError> {
        if self.type_mappings.contains_key(&type_info.type_name) {
            return Err(ReError::String(format!(
                "Custom type '{}' already registered",
                type_info.type_name
            )));
        }

        self.type_mappings.insert(type_info.type_name.clone(), type_info);
        Ok(())
    }

    /// Get a custom decoder by type name
    pub fn get_decoder(&self, type_name: &str) -> Option<&dyn CustomTypeDecoder> {
        self.decoders.get(type_name).map(|d| d.as_ref())
    }

    /// Get custom type info by name
    pub fn get_type_info(&self, type_name: &str) -> Option<&CustomTypeInfo> {
        self.type_mappings.get(type_name)
    }

    /// List all registered custom types
    pub fn list_custom_types(&self) -> Vec<String> {
        self.type_mappings.keys().cloned().collect()
    }

    /// Get statistics about custom types
    pub fn get_stats(&self) -> CustomTypeStats {
        CustomTypeStats {
            total_decoders: self.decoders.len(),
            total_types: self.type_mappings.len(),
        }
    }
}

#[derive(Debug)]
pub struct CustomTypeStats {
    pub total_decoders: usize,
    pub total_types: usize,
}

// Dummy decoder for placeholder purposes
struct DummyDecoder;

impl CustomTypeDecoder for DummyDecoder {
    fn decode_custom_type(&self, _data: &[u8], _type_info: &CustomTypeInfo) -> Result<ColumnValue, ReError> {
        Err(ReError::String("Dummy decoder should not be called".to_string()))
    }

    fn supported_types(&self) -> Vec<String> {
        vec![]
    }

    fn decoder_info(&self) -> CustomDecoderInfo {
        CustomDecoderInfo {
            name: "Dummy".to_string(),
            version: "0.0.0".to_string(),
            description: "Placeholder decoder".to_string(),
            author: None,
        }
    }
}

impl Default for CustomTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point2d_decoder() {
        let decoder = Point2DDecoder;
        
        // Create test data: x=1.0, y=2.0
        let mut data = Vec::new();
        data.extend_from_slice(&1.0f64.to_le_bytes());
        data.extend_from_slice(&2.0f64.to_le_bytes());
        
        let type_info = CustomTypeInfo {
            type_name: "Point2D".to_string(),
            type_id: 200,
            metadata: HashMap::new(),
            version: 1,
        };
        
        let result = decoder.decode_custom_type(&data, &type_info).unwrap();
        
        if let ColumnValue::Custom { type_name, metadata, .. } = result {
            assert_eq!(type_name, "Point2D");
            assert_eq!(metadata.get("x").unwrap(), "1");
            assert_eq!(metadata.get("y").unwrap(), "2");
        } else {
            panic!("Expected custom column value");
        }
    }

    #[test]
    fn test_custom_type_registry() {
        let mut registry = CustomTypeRegistry::new();
        
        let type_info = CustomTypeInfo {
            type_name: "Point2D".to_string(),
            type_id: 200,
            metadata: HashMap::new(),
            version: 1,
        };
        
        registry.register_custom_type(type_info).unwrap();
        
        let decoder = Box::new(Point2DDecoder);
        registry.register_custom_decoder(decoder).unwrap();
        
        assert!(registry.get_type_info("Point2D").is_some());
        assert_eq!(registry.list_custom_types(), vec!["Point2D"]);
    }
}
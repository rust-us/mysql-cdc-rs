use std::collections::HashMap;

/// Column metadata containing type information and parsing hints
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// MySQL column type identifier
    pub column_type: u8,
    /// Type-specific metadata (length, precision, etc.)
    pub metadata: u16,
    /// Character set information for string types
    pub charset: Option<u16>,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Whether the column is unsigned (for numeric types)
    pub unsigned: bool,
    /// Additional metadata from TABLE_MAP_EVENT
    pub extra_metadata: HashMap<String, Vec<u8>>,
}

impl ColumnMetadata {
    pub fn new(column_type: u8, metadata: u16) -> Self {
        Self {
            column_type,
            metadata,
            charset: None,
            nullable: true,
            unsigned: false,
            extra_metadata: HashMap::new(),
        }
    }

    pub fn with_charset(mut self, charset: u16) -> Self {
        self.charset = Some(charset);
        self
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_unsigned(mut self, unsigned: bool) -> Self {
        self.unsigned = unsigned;
        self
    }

    pub fn add_extra_metadata(mut self, key: String, value: Vec<u8>) -> Self {
        self.extra_metadata.insert(key, value);
        self
    }
}
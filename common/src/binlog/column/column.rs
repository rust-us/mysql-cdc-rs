use std::borrow::Cow;
use crate::binlog::column::column_type::SrcColumnType;

/// Represents MySql Column (column packet).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SrcColumn {
    catalog: String,
    schema: Vec<u8>,
    table: Vec<u8>,
    org_table: Vec<u8>,
    name: Vec<u8>,
    org_name: Vec<u8>,
    fixed_length_fields_len: u8,
    column_length: u32,
    character_set: u16,
    column_type: SrcColumnType,
    flags: u16,
    decimals: u8,
    __filler: u16,
    // COM_FIELD_LIST is deprecated, so we won't support it
}

impl SrcColumn {
    pub fn new(column_type: SrcColumnType) -> Self {
        Self {
            catalog: Default::default(),
            schema: Default::default(),
            table: Default::default(),
            org_table: Default::default(),
            name: Default::default(),
            org_name: Default::default(),
            fixed_length_fields_len: Default::default(),
            column_length: Default::default(),
            character_set: Default::default(),
            flags: Default::default(),
            column_type,
            decimals: Default::default(),
            __filler: 0,
        }
    }

    pub fn with_schema(mut self, schema: &[u8]) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn with_table(mut self, table: &[u8]) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_org_table(mut self, org_table: &[u8]) -> Self {
        self.org_table = org_table.into();
        self
    }

    pub fn with_name(mut self, name: &[u8]) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_org_name(mut self, org_name: &[u8]) -> Self {
        self.org_name = org_name.into();
        self
    }

    pub fn with_flags(mut self, flags: u16) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_column_length(mut self, column_length: u32) -> Self {
        self.column_length = column_length;
        self
    }

    pub fn with_character_set(mut self, character_set: u16) -> Self {
        self.character_set = character_set;
        self
    }

    pub fn with_decimals(mut self, decimals: u8) -> Self {
        self.decimals = decimals;
        self
    }

    /// Returns value of the column_length field of a column packet.
    ///
    /// Can be used for text-output formatting.
    pub fn column_length(&self) -> u32 {
        self.column_length
    }

    /// Returns value of the column_type field of a column packet.
    pub fn column_type(&self) -> SrcColumnType {
        self.column_type.clone()
    }

    /// Returns value of the character_set field of a column packet.
    pub fn character_set(&self) -> u16 {
        self.character_set
    }

    /// Returns value of the flags field of a column packet.
    pub fn flags(&self) -> u16 {
        self.flags
    }

    /// Returns value of the decimals field of a column packet.
    ///
    /// Max shown decimal digits. Can be used for text-output formatting
    ///
    /// *   `0x00` for integers and static strings
    /// *   `0x1f` for dynamic strings, double, float
    /// *   `0x00..=0x51` for decimals
    pub fn decimals(&self) -> u8 {
        self.decimals
    }

    /// Returns value of the schema field of a column packet as a byte slice.
    pub fn schema_ref(&self) -> &[u8] {
        &*self.schema
    }

    /// Returns value of the schema field of a column packet as a string (lossy converted).
    pub fn schema_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.schema_ref())
    }

    /// Returns value of the table field of a column packet as a byte slice.
    pub fn table_ref(&self) -> &[u8] {
        &*self.table
    }

    /// Returns value of the table field of a column packet as a string (lossy converted).
    pub fn table_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.table_ref())
    }

    /// Returns value of the org_table field of a column packet as a byte slice.
    ///
    /// "org_table" is for original table name.
    pub fn org_table_ref(&self) -> &[u8] {
        &*self.org_table
    }

    /// Returns value of the org_table field of a column packet as a string (lossy converted).
    pub fn org_table_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.org_table_ref())
    }

    /// Returns value of the name field of a column packet as a byte slice.
    pub fn name_ref(&self) -> &[u8] {
        &*self.name
    }

    /// Returns value of the name field of a column packet as a string (lossy converted).
    pub fn name_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.name_ref())
    }

    /// Returns value of the org_name field of a column packet as a byte slice.
    ///
    /// "org_name" is for original column name.
    pub fn org_name_ref(&self) -> &[u8] {
        &*self.org_name
    }

    /// Returns value of the org_name field of a column packet as a string (lossy converted).
    pub fn org_name_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.org_name_ref())
    }
}

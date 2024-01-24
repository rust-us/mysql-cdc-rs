use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use log::debug;

use mysql_common::Row;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::OnceCell;
use regex::Regex;

use crate::err::CResult;
use crate::err::decode_error::ReError;
use crate::schema::data_type::{DataType, TableSchema};

pub type CatalogRef = Arc<RwLock<Catalog>>;
pub type SchemaRef = Arc<RwLock<Schema>>;
pub type TableRef = Arc<RwLock<Table>>;
pub type ColumnRef = Arc<Column>;

// metadata instance
// TODO, long live structure need a special memory allocator to avoid memory fragmentation
pub static mut METADATA_INSTANCE: OnceCell<Metadata> = OnceCell::new();

pub static OP_DEFAULT_CATALOG_NAME: OnceCell<String> = OnceCell::new();

pub struct Metadata {
    write_lock: Mutex<()>,
    catalog: HashMap<String, CatalogRef>,
}

#[derive(Debug)]
pub struct Catalog {
    pub name: String,
    pub schema: HashMap<String, SchemaRef>,
    // pub schema_id_map: HashMap<i32, String>,
}

#[derive(Debug)]
pub struct Schema {
    pub schema_id: String,
    pub name: String,
    pub tables: HashMap<String, TableRef>,
    // pub table_id_map: HashMap<i32, String>,
}

#[derive(Debug)]
pub struct Table {
    pub table_id: i32,
    pub name: String,
    pub physical_name: String,
    pub schema_id: String,
    //pub schema: Box<Schema>,
    pub columns: Vec<ColumnRef>,
    pub pk_column_idx: Vec<usize>,
    pub distribute_type: DistributeType,
    pub distribution_key_column_idx: Vec<usize>,

    pub version: i64,
    pub is_materialized: bool,

    //省略 分区，索引，version, comment，其他property
    pub catalog_name: String,
    pub schema_name: String,
}

#[derive(Debug)]
pub struct Column {
    pub column_id: i32,
    pub name: String,
    pub physical_name: String,
    pub data_type: DataType,
    pub ordinal_position: i32,

    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,

    //todo default value
    pub default: Option<String>,
    pub default_value_is_current_timestamp: bool,
    pub default_value_is_current_date: bool,

    //是否生成列
    pub is_generated_column: bool,

    //decimal属性
    pub precision: i32,
    pub scale: i32,
}

#[derive(IntoPrimitive, TryFromPrimitive, Copy, Clone, Debug)]
#[repr(i32)]
pub enum DistributeType {
    HASH,
    BROADCAST,
}

/// map error macro_rules
/// map a Result<T, LockError> into Result<T, ReError>
macro_rules! mle {
    ($r: expr) => {
        $r.map_err(|e| ReError::OpMetadataErr(format!("require lock failed, {}", e)))
    };
}

impl Metadata {
    #[inline]
    pub fn get_catalog(&self, catalog: &String) -> Option<CatalogRef> {
        self.catalog.get(catalog).map(|c| c.clone())
    }

    pub fn get_schema(&self, catalog: &String, schema: &String) -> CResult<Option<SchemaRef>> {
        if let Some(c) = self.get_catalog(catalog) {
            let catalog = c.read()
                .map_err(|_| ReError::OpMetadataErr("get_schema err".into()))?;
            return Ok(catalog.get_schema(schema));
        }
        return Ok(None);
    }

    pub fn get_table(&self, catalog: &String, schema: &String, table: &String) -> CResult<Option<TableRef>> {
        if let Some(schema) = self.get_schema(catalog, schema)? {
            let table = schema.read()
                .map_err(|_| ReError::OpMetadataErr("get_schema err".into()))?
                .get_table(table);
            return Ok(table);
        }
        return Ok(None);
    }

    #[inline]
    pub fn is_table_exist(&self, catalog: &String, schema: &String, table: &String) -> CResult<bool> {
        Ok(self.get_table(catalog, schema, table)?.is_some())
    }


    pub fn existing_table_names(&self) -> CResult<Vec<TableSchema>> {
        let mut tables = vec![];
        for (_, c) in &self.catalog {
            let catalog = mle!(c.read())?;
            for (schema_name, s) in &catalog.schema {
                let schema = mle!(s.read())?;
                for (table_name, _t) in &schema.tables {
                    tables.push(TableSchema::create(&catalog.name, schema_name, &table_name));
                }
            }
        }
        Ok(tables)
    }

    pub fn insert_table(&mut self, table: Table) -> CResult<()> {
        let mut c = self.catalog.entry(table.catalog_name.clone()).or_insert_with(|| {
            Arc::new(RwLock::new(Catalog {
                name: table.catalog_name.clone(),
                schema: HashMap::new(),
                // schema_id_map: HashMap::new(),
            }))
        });
        let mut catalog = mle!(c.write())?;
        let s = catalog.schema.entry(table.schema_name.clone()).or_insert_with(|| {
            Arc::new(RwLock::new(Schema {
                schema_id: table.schema_id.clone(),
                name: table.schema_name.clone(),
                tables: HashMap::new(),
                // table_id_map: HashMap::new(),
            }))
        });
        let mut schema = mle!(s.write())?;
        schema.tables.insert(table.name.clone(), Arc::new(RwLock::new(table)));
        Ok(())
    }


    /// remove table from metadata
    pub fn remove_table(&mut self, catalog: &String, schema: &String, table: &String) -> CResult<bool> {
        if let Some(c) = self.get_catalog(catalog) {
            let mut catalog = mle!(c.write())?;
            if let Some(s) = catalog.get_schema(schema) {
                let mut schema = mle!(s.write())?;
                // remove from table
                if let Some(t) = schema.tables.remove(table) {
                    let table = mle!(t.read())?;
                    // let _ = schema.table_id_map.remove(&table.table_id);
                    // remove schema from catalog if this schema is empty
                    if schema.is_empty() {
                        catalog.schema.remove(&schema.name);
                        // catalog.schema_id_map.remove(&schema.schema_id);
                    }
                    // remove catalog from metadata if this catalog is empty
                    if catalog.is_empty() {
                        self.catalog.remove(&catalog.name);
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

impl Catalog {
    pub fn add_schema(&mut self, schema: Schema) {
        self.schema.insert(schema.name.clone(), Arc::new(RwLock::new(schema)));
    }

    pub fn add_table(&mut self, table: Table) -> CResult<()> {
        match self.schema.get_mut(&table.name) {
            Some(mut schema) => {
                let tid = table.table_id;
                let name = table.name.clone();
                let mut schema = schema
                    .write()
                    .map_err(|e| {
                        ReError::OpMetadataErr(format!("lock schema {} err, {}", name.clone(), e.to_string()))
                    })?;
                schema.tables.insert(name.clone(), Arc::new(RwLock::new(table)));
                // schema.table_id_map.insert(tid, name);
                Ok(())
            }
            None => {
                Err(ReError::OpSchemaNotExistErr(
                    format!("table's schema not exists {:?}", &table)))
            }
        }
    }

    pub fn get_schema(&self, name: &String) -> Option<SchemaRef> {
        self.schema.get(name).map(|s| s.clone())
    }

    // #[inline]
    // pub fn get_schema_by_id(&self, id: i32) -> Option<SchemaRef> {
    //     self.schema_id_map
    //         .get(&id)
    //         .map(|name| self.schema.get(name)).flatten()
    //         .map(|s| s.clone())
    // }

    // pub fn get_table(&self, schema_id: i32, table_id: i32) -> CResult<Option<TableRef>> {
    //     if let Some(schema_ref) = self.get_schema_by_id(schema_id) {
    //         let t = schema_ref
    //             .read()
    //             .map_err(|e| {
    //                 ReError::OpMetadataErr(
    //                     format!("get_table schema_id: {}, table_id: {} lock err: {}", schema_id, table_id, e))
    //             })?
    //             .get_table_by_id(table_id);
    //         Ok(t)
    //     } else {
    //         Ok(None)
    //     }
    // }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.schema.is_empty()
    }
}

impl Schema {
    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), Arc::new(RwLock::new(table)));
    }

    pub fn get_table(&self, table_name: &String) -> Option<TableRef> {
        self.tables.get(table_name).map(|t| t.clone())
    }

    // #[inline]
    // pub fn get_table_by_id(&self, table_id: i32) -> Option<TableRef> {
    //     self.table_id_map
    //         .get(&table_id)
    //         .map(|t| self.tables.get(t))
    //         .flatten()
    //         .map(|t| t.clone())
    // }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl Table {
    #[inline]
    pub fn get_column_by_name(&self, column_name: &str) -> Option<ColumnRef> {
        self.columns.iter().find(|&c| c.name == column_name).map(|c| c.clone())
    }

    #[inline]
    pub fn get_column(&self, column_id: i32) -> Option<ColumnRef> {
        self.columns.iter().find(|&c| c.column_id == column_id).map(|c| c.clone())
    }

    #[inline]
    pub fn get_table_schema(&self) -> TableSchema {
        TableSchema::create(&self.catalog_name, &self.schema_name, &self.name)
    }
}

/// parse table rows into (Table, schema_name, Vec<distribute_column>, Vec<primary_key_column>)
pub fn try_parse_table(row: Row) -> Result<(Table, String, Vec<String>, Vec<String>), ReError> {
    if row.len() != 10 {
        return Err(ReError::OpMetadataErr("Parse Table: TABLES err, fields len not equals 9".into()));
    }
    let mut i = 0;
    let schema_id: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field TABLE_SCHEMA_ID can not be null".into())
    })?;
    i += 1;
    let table_schema: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field TABLE_SCHEMA can not be null".into())
    })?;
    i += 1;
    let table_id: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field TABLE_ID can not be null".into())
    })?;
    i += 1;
    let table_name: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field TABLE_NAME can not be null".into())
    })?;
    i += 1;
    let physical_table_name: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field PHYSICAL_TABLE_NAME can not be null".into())
    })?;
    i += 1;
    let distribute_type: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field DISTRIBUTE_TYPE can not be null".into())
    })?;
    i += 1;
    let distribute_column: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field DISTRIBUTE_COLUMN can not be null".into())
    })?;
    i += 1;
    let primary_columns: String = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field PRIMARYKEY_COLUMNS can not be null".into())
    })?;
    i += 1;
    let current_version: i64 = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field CURRENT_VERSION can not be null".into())
    })?;
    i += 1;
    let is_materialized: i8 = row.get(i).ok_or_else(|| {
        ReError::OpMetadataErr("Parse Table: TABLES err, field IS_MATERIALIZED can not be null".into())
    })?;
    i += 1;
    let table_id = i32::from_str_radix(table_id.as_str(), 10)
        .map_err(|_| {
            ReError::OpMetadataErr(format!(
                "Parse Table: TABLES err, field TABLE_ID: {} not illegal", &table_id
            ))
        })?;
    let distribute_type = DistributeType::try_from(distribute_type)?;
    // distribute_column, primary_columns
    let distribute_column = distribute_column.split(",").map(|c| String::from(c)).collect();
    let primary_columns = primary_columns.split(",").map(|c| String::from(c)).collect();
    let catalog_name = "def".into();
    let (schema_name, parsed_table_name, parsed_table_id) = parse_physical_table_name(physical_table_name.as_str())?;
    if !parsed_table_name.eq(&table_name) {
        return Err(ReError::OpMetadataErr(format!(
            "parse physical_table_name: {} not matched table_name: {}", &physical_table_name, &table_name
        )));
    }
    if parsed_table_id != table_id {
        return Err(ReError::OpMetadataErr(format!(
            "parse parsed_table_id: {} not matched table_id: {}", parsed_table_id, table_id
        )));
    }
    Ok((Table {
        table_id,
        name: table_name,
        physical_name: physical_table_name,
        schema_id,
        columns: vec![],
        pk_column_idx: vec![],
        distribute_type,
        distribution_key_column_idx: vec![],
        version: current_version,
        is_materialized: is_materialized == 1,
        catalog_name,
        schema_name,
    }, table_schema, distribute_column, primary_columns))
}


/// parse physical_table_name into schema name, table name, table id
/// eg: physical_table_name: test##abc##32, schema name: test, table name: abc, table id: 32
fn parse_physical_table_name(name: &str) -> CResult<(String, String, i32)> {
    let reg = Regex::new(r"^([^#]+)##([^#]+)##(\d+)$").map_err(|e| {
        ReError::OpMetadataErr(format!(
            "parse_physical_table_name failed, input: {}", name
        ))
    })?;
    if let Some(cap) = reg.captures(name) {
        if cap.len() == 4 {
            let (_, [schema, table, id]) = cap.extract();
            let id = i32::from_str_radix(id, 10).map_err(|_| {
                ReError::OpMetadataErr(format!(
                    "parse_physical_table_name table_id failed, input: {}", name
                ))
            })?;
            return Ok((schema.into(), table.into(), id));
        }
        return Err(ReError::OpMetadataErr(format!(
            "parse_physical_table_name regex group match failed, input: {}", name
        )));
    }
    Err(ReError::OpMetadataErr(format!(
        "parse_physical_table_name regex match failed, input: {}", name
    )))
}

unsafe impl Sync for Metadata {}

unsafe impl Send for Metadata {}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            write_lock: Mutex::new(()),
            catalog: HashMap::new(),
        }
    }
}

/// parse row to column
impl TryFrom<Row> for Column {
    type Error = ReError;

    // column orders:
    // 0. TABLE_SCHEMA,
    // 1. TABLE_NAME,
    // 2. COLUMN_NAME,
    // 3. PHYSICAL_COLUMN_NAME,
    // 4. DATA_TYPE,
    // 5. ORDINAL_POSITION,
    // 6. IS_NULLABLE,
    // 7. AUTO_INCREMENT,
    // 8. DEFAULT_VALUE,
    // 9. RAW_DATA_TYPE,
    // 10. IS_PRIMARYKEY
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        if row.len() != 11 {
            return Err(ReError::OpMetadataErr("Parse Table: Columns err, fields len not equals 11".into()));
        }
        let _table_schema: String = row.get(0).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field TABLE_SCHEMA can not be null".into())
        })?;
        let _table_name: String = row.get(1).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field TABLE_NAME can not be null".into())
        })?;
        let _column_name: String = row.get(2).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field COLUMN_NAME can not be null".into())
        })?;
        let physical_column_name: String = row.get(3).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field PHYSICAL_COLUMN_NAME can not be null".into())
        })?;
        let data_type: i32 = row.get(4).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field DATA_TYPE can not be null".into())
        })?;
        let ordinal_position: i32 = row.get(5).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field ORDINAL_POSITION can not be null".into())
        })?;
        let is_null: i8 = row.get(6).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field IS_NULLABLE can not be null".into())
        })?;
        let auto_increment: i8 = row.get(7).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field AUTO_INCREMENT can not be null".into())
        })?;
        let default_value: Option<String> = row.get(8).expect("metadata parse column, index 8 out of range");
        let raw_data_type: String = row.get(9).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field RAW_DATA_TYPE can not be null".into())
        })?;
        let is_primary_key: i8 = row.get(10).ok_or_else(|| {
            ReError::OpMetadataErr("Parse Table: Columns err, field IS_PRIMARYKEY can not be null".into())
        })?;
        // parse fields
        let (column_name, column_id) = match physical_column_name.split_once("##") {
            Some((column_name, column_id)) => {
                (String::from(column_name), i32::from_str_radix(column_id, 10).map_err(|e| {
                    ReError::OpMetadataErr(format!(
                        "Parse Table: Columns err, field PHYSICAL_COLUMN_NAME: {} not illegal", &physical_column_name
                    ))
                })?)
            }
            None => {
                return Err(ReError::OpMetadataErr(format!(
                    "Parse Table: Columns err, field PHYSICAL_COLUMN_NAME: {} not illegal", &physical_column_name
                )));
            }
        };
        let data_type = DataType::try_from(data_type).map_err(|_| {
            ReError::OpMetadataErr(format!(
                "Parse Table: Columns err, data_type: {} not illegal", data_type
            ))
        })?;
        let (precision, scale) = match data_type {
            DataType::Decimal
            | DataType::ByteArray
            | DataType::ShortArray
            | DataType::IntArray
            | DataType::FloatArray
            | DataType::StringArray
            | DataType::DoubleArray
            | DataType::LongArray => {
                parse_precision_scale(raw_data_type.as_str())?
            }
            _ => {
                (0, 0)
            }
        };
        Ok(Self {
            column_id,
            name: column_name,
            physical_name: physical_column_name,
            data_type,
            ordinal_position,
            nullable: is_null == 1,
            primary_key: is_primary_key == 1,
            auto_increment: auto_increment == 1,
            default: default_value,
            default_value_is_current_timestamp: matches!(data_type, DataType::Timestamp),
            default_value_is_current_date: matches!(data_type, DataType::Date),
            is_generated_column: false,
            precision,
            scale,
        })
    }
}

pub fn get_metadata() -> &'static Metadata {
    unsafe {
        METADATA_INSTANCE.get_or_init(|| {
            Metadata::default()
        })
    }
}

pub fn get_metadata_mut() -> &'static mut Metadata {
    unsafe {
        METADATA_INSTANCE.get_mut().expect("metadata must init")
    }
}

fn parse_precision_scale(s: &str) -> CResult<(i32, i32)> {
    #[inline]
    fn _err(s: &str) -> ReError {
        ReError::OpMetadataErr(format!(
            "parse_precision_scale error, input: {}", s
        ))
    }

    let reg = Regex::new(r"^([\w<>]+)(\((\d+)(,(\d+))?\))?$").unwrap();
    match reg.captures(s) {
        Some(cap) => {
            let (precision, scale) = if cap.len() == 6 {
                let data_type = cap.get(1).map(|m| m.as_str())
                    .expect(format!("parse_precision_scale error, can not get data_type from input {}", s).as_str());
                let precision = cap.get(3).map(|m| m.as_str()).unwrap_or_else(|| {
                    if data_type.eq_ignore_ascii_case("decimal") {
                        "10"
                    } else {
                        "0"
                    }
                });
                let scale = cap.get(5).map(|m| m.as_str()).unwrap_or("0");
                (precision, scale)
            } else {
                return Err(_err(s));
            };
            let precision = i32::from_str_radix(
                precision, 10,
            ).map_err(|_| _err(s))?;
            let scale = i32::from_str_radix(
                scale, 10,
            ).map_err(|_| _err(s))?;
            Ok((precision, scale))
        }
        None => {
            Err(_err(s))
        }
    }
}

impl TryFrom<String> for DistributeType {
    type Error = ReError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.eq_ignore_ascii_case("hash") {
            Ok(DistributeType::HASH)
        } else if s.eq_ignore_ascii_case("BROADCAST") {
            Ok(DistributeType::BROADCAST)
        } else {
            Err(ReError::OpMetadataErr(format!(
                "illegal DistributeType: {}", &s
            )))
        }
    }
}

pub fn op_default_catalog() -> &'static String {
    unsafe {
        OP_DEFAULT_CATALOG_NAME.get_or_init(|| {
            String::from("def")
        })
    }
}

#[cfg(test)]
mod test {
    use crate::err::CResult;
    use crate::log::init_test_log;
    use crate::schema::schema::{parse_physical_table_name, parse_precision_scale};

    #[test]
    fn test_pattern() -> CResult<()> {
        init_test_log();
        {
            let raw_type = "decimal";
            // decimal default 10, 0 in OP
            let (precision, scale) = parse_precision_scale(raw_type)?;
            assert_eq!(10, precision);
            assert_eq!(0, scale);
        }
        {
            let raw_type = "decimal(10,20)";
            let (precision, scale) = parse_precision_scale(raw_type)?;
            assert_eq!(10, precision);
            assert_eq!(20, scale);
        }
        {
            let raw_type = "decimal(8)";
            let (precision, scale) = parse_precision_scale(raw_type)?;
            assert_eq!(8, precision);
            assert_eq!(0, scale);
        }
        {
            let raw_type = "array<float>";
            let (precision, scale) = parse_precision_scale(raw_type)?;
            assert_eq!(0, precision);
            assert_eq!(0, scale);
        }
        {
            let raw_type = "array<float>(6)";
            let (precision, scale) = parse_precision_scale(raw_type)?;
            assert_eq!(6, precision);
            assert_eq!(0, scale);
        }
        {
            let raw_type = "float(9)";
            let (precision, scale) = parse_precision_scale(raw_type)?;
            assert_eq!(9, precision);
            assert_eq!(0, scale);
        }
        Ok(())
    }

    #[test]
    fn test_parse_physical_table_name() -> CResult<()> {
        {
            let name = "a##b##12";
            let (s, t, id) = parse_physical_table_name(name)?;
            assert_eq!("a", s);
            assert_eq!("b", t);
            assert_eq!(12, id);
        }

        {
            let name = "abc_1293##aoai-20Z9182_xjx##09283";
            let (s, t, id) = parse_physical_table_name(name)?;
            assert_eq!("abc_1293", s);
            assert_eq!("aoai-20Z9182_xjx", t);
            assert_eq!(9283, id);
        }

        {
            let name = "abc_1293###aoai-20Z9182_xjx##09283";
            let result = parse_physical_table_name(name);
            assert!(result.is_err());
        }

        {
            let name = "abc_1293##aoai-20Z9182_xjx##0xx9283";
            let result = parse_physical_table_name(name);
            assert!(result.is_err());
        }
        Ok(())
    }
}
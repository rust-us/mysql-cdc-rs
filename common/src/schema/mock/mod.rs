use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use toml::Value;
use tracing::debug;

use crate::err::CResult;
use crate::err::decode_error::ReError;
use crate::schema::data_type::DstColumnType;
use crate::schema::schema::{Column, ColumnRef, DistributeType, Metadata, Table};

fn get_int(e: &Value, name: &str) -> i64 {
    e.get(name)
        .expect(format!("column {} not exists, parsing: {:?}", name, e).as_str())
        .as_integer()
        .expect(format!("column {} not exists, parsing: {:?}", name, e).as_str())
}

fn get_int_opt(e: &Value, name: &str) -> Option<i64> {
    e.get(name)
        .map(|v|v.as_integer())
        .flatten()
}

fn get_bool_opt(e: &Value, name: &str) -> Option<bool> {
    e.get(name)
        .map(|v|v.as_bool())
        .flatten()
}

fn get_string(e: &Value, name: &str) -> String {
    e.get(name)
        .expect(format!("column {} not exists, parsing: {:?}", name, e).as_str())
        .as_str()
        .expect(format!("column {} not exists, parsing: {:?}", name, e).as_str())
        .into()
}

fn get_string_opt(e: &Value, name: &str) -> Option<String> {
    e.get(name)
        .map(|v|v.as_str())
        .flatten()
        .map(|s|s.into())
}

fn parse_column_from_toml(name: &String, e: &Value, pk_columns: &Vec<String>) -> Result<Column, ReError> {
    let column_id = get_int(e, "id") as i32;
    let physical_name = format!("{}##{}", &name, column_id);
    let data_type_str = get_string(e, "data_type");
    let data_type = DstColumnType::try_from(data_type_str)?;
    let nullable = get_bool_opt(e, "nullable").unwrap_or(true);
    let auto_increment = get_bool_opt(e, "auto_increment").unwrap_or(false);
    let default = get_string_opt(e, "default");
    let precision = get_int_opt(e, "precision").unwrap_or(0) as i32;
    let scale = get_int_opt(e, "scale").unwrap_or(0) as i32;
    let primary_key = pk_columns.contains(name);
    Ok(Column {
        column_id,
        name: name.clone(),
        physical_name,
        data_type,
        ordinal_position: column_id,
        nullable,
        primary_key,
        auto_increment,
        default,
        default_value_is_current_timestamp: false,
        default_value_is_current_date: false,
        is_generated_column: false,
        precision,
        scale,
    })
}

impl Metadata {
    pub fn from_mock<P: AsRef<Path>>(path: P) -> CResult<Metadata> {
        let mut file = File::open(path)
            .map_err(|e| ReError::ConfigFileParseErr(e.to_string()))?;
        let mut s = String::new();
        let len = file.read_to_string(&mut s)
            .map_err(|e| ReError::ConfigFileParseErr(e.to_string()))?;
        assert!(len > 0, "read file len 0");
        let catalogs: toml::Table = toml::from_str(s.as_str())
            .map_err(|e| ReError::ConfigFileParseErr(e.to_string()))?;
        debug!("mock metadata: {:?}", catalogs);
        let mut metadata = Metadata::default();
        // parse catalog
        for (catalog_name, catalog) in catalogs {
            assert!(catalog.is_table(), "mock catalog must be Table Type");
            let catalog = catalog.as_table().unwrap();
            for (schema_name, schema) in catalog {
                assert!(schema.is_table(), "mock schema must be Table Type");
                let schema = schema.as_table().unwrap();
                for (table_name, table) in schema {
                    assert!(table.is_table(), "mock table must be Table Type");
                    let table = table.as_table().unwrap();
                    let table_id = table.get("table_id")
                        .expect(format!("mock table {} must has table_id", &table_name).as_str())
                        .as_integer()
                        .expect(format!("mock table {} table_id must a integer", &table_name).as_str())
                        as i32;
                    let physical_name = table.get("physical_name")
                        .expect(format!("mock table {} must has physical_name", &table_name).as_str())
                        .as_str()
                        .expect(format!("mock table {} physical_name must a str", &table_name).as_str());
                    let distribute_type = table.get("distribute_type")
                        .expect(format!("mock table {} must has table_id", &table_name).as_str())
                        .as_str()
                        .expect(format!("mock table {} distribute_type must a str", &table_name).as_str());
                    let distribute_type = DistributeType::try_from(distribute_type)?;
                    let pk_columns: Vec<String> = table.get("pk_columns")
                        .expect(format!("mock table {} must has pk_columns", &table_name).as_str())
                        .as_array()
                        .expect(format!("mock table {} pk_columns must a Vec", &table_name).as_str())
                        .iter()
                        .map(|v| v.as_str().expect(format!("mock table {} pk_columns must a Vec str", &table_name).as_str()).into())
                        .collect();
                    let distribution_key_columns: Vec<String> = table.get("distribution_key_columns")
                        .expect(format!("mock table {} must has distribution_key_column_idx", &table_name).as_str())
                        .as_array()
                        .expect(format!("mock table {} distribution_key_column_idx must a Vec", &table_name).as_str())
                        .iter()
                        .map(|v| v.as_str().expect(format!("mock table {} distribution_key_column_idx must a Vec str", &table_name).as_str()).into())
                        .collect();
                    let columns = table["columns"]
                        // .expect(format!("mock table {} must has columns", &table_name).as_str())
                        .as_table()
                        .expect(format!("mock table {} columns must be Table type", &table_name).as_str());
                    let mut meta_columns = vec![];
                    for (column_name, column) in columns {
                        let column_meta = parse_column_from_toml(column_name, column, &pk_columns)?;
                        meta_columns.push(Arc::new(column_meta));
                    }
                    fn find_idx(column_names: &Vec<String>, meta_columns: &Vec<ColumnRef>, ex: &str) -> Vec<usize> {
                        column_names.iter().map(|cn| {
                            meta_columns.iter().find_map(|c| {
                                if &c.name == cn {
                                    Some(c.column_id as usize)
                                } else {
                                    None
                                }
                            }).expect(format!("{}: {:?} not exists", ex, column_names).as_str())
                        }).collect()
                    }
                    let pk_column_idx: Vec<usize> = find_idx(&pk_columns, &meta_columns, "pk_columns");
                    let distribution_key_column_idx: Vec<usize> = find_idx(&distribution_key_columns, &meta_columns, "distribution_key_columns");
                    let meta_table = Table {
                        table_id,
                        name: table_name.clone(),
                        physical_name: physical_name.into(),
                        schema_id: "0".to_string(),
                        columns: meta_columns,
                        pk_column_idx,
                        distribute_type,
                        distribution_key_column_idx,
                        version: 0,
                        is_materialized: false,
                        catalog_name: catalog_name.clone(),
                        schema_name: schema_name.clone(),
                    };
                    debug!("metadata table: {:?}", meta_table);
                    metadata.insert_table(meta_table)?;
                }
            }
        }
        Ok(metadata)
    }
}
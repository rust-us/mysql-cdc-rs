use serde::Serialize;
use crate::column::column_type::ColumnType;
use crate::events::BuildType;
use crate::metadata::table_metadata::TableMetadata;

/// dump insert 数据之前声明的表结构信息
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct DumpTableMapEvent {
    /// schema
    pub database_name: String,

    pub table_name: String,

    /// len encoded integer
    pub columns_number: u64,

    /// Gets columns metadata 字段类型， 每个枚举的值与column_types 对应
    pub column_metadata_type: Vec<ColumnType>,

    /// Gets columns nullability
    pub null_bitmap: Vec<u8>,

    /// Gets table metadata for MySQL 5.6+
    pub table_metadata: Option<TableMetadata>,

    /// 构造来源： BINLOG、DUMP
    pub build_type: BuildType,
}

impl DumpTableMapEvent {
    pub fn new(database_name: String, table_name: String, columns_number: u64, column_metadata_type: Vec<ColumnType>,
               null_bitmap: Vec<u8>, table_metadata: Option<TableMetadata>) -> Self {
        DumpTableMapEvent {
            database_name,
            table_name,
            columns_number,
            column_metadata_type,
            null_bitmap,
            table_metadata,
            build_type: BuildType::DUMP,
        }
    }
}
use serde::Serialize;
use common::binlog::column::column_type::SrcColumnType;
use crate::events::BuildType;

/// dump insert 数据之前声明的表结构信息
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct DumpTableMapEvent {
    /// schema
    pub database_name: String,

    pub table_name: String,

    /// len encoded integer
    pub columns_number: u64,

    /// Gets columns metadata 字段类型， 每个枚举的值与column_types 对应
    pub column_metadata_type: Vec<SrcColumnType>,

    /// Gets columns nullability， 用于标识某一列是否允许为 null。 0 表示不允许为null, 非0表示允许为null
    pub null_bitmap: Vec<u8>,

    /// table metadata for MySQL 5.6+， field default_charset, default value 33 --> utf8
    /// pos:  TableMetadata[default_charset[default_charset]]
    pub default_charset: u32,

    /// 构造来源： BINLOG、DUMP
    pub build_type: BuildType,
}

impl DumpTableMapEvent {
    pub fn new(database_name: String, table_name: String, columns_number: u64, column_metadata_type: Vec<SrcColumnType>,
               null_bitmap: Vec<u8>, default_charset: u32) -> Self {
        DumpTableMapEvent {
            database_name,
            table_name,
            columns_number,
            column_metadata_type,
            null_bitmap,
            default_charset,
            build_type: BuildType::DUMP,
        }
    }
}
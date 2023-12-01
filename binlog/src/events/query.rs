use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum QueryStatusVar {
    Q_FLAGS2_CODE(Q_FLAGS2_CODE_VAL),
    Q_SQL_MODE_CODE(Q_SQL_MODE_CODE_VAL),
    Q_CATALOG(String),
    Q_AUTO_INCREMENT(u16, u16),
    Q_CHARSET_CODE(u16, u16, u16),
    Q_TIME_ZONE_CODE(String),
    Q_CATALOG_NZ_CODE(String),
    Q_LC_TIME_NAMES_CODE(u16),
    Q_CHARSET_DATABASE_CODE(u16),
    Q_TABLE_MAP_FOR_UPDATE_CODE(u64),
    Q_MASTER_DATA_WRITTEN_CODE(u32),
    Q_INVOKERS(String, String),
    Q_UPDATED_DB_NAMES(Vec<String>),
    // NOTE this field take 3 bytes
    Q_MICROSECONDS(u32),
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Q_FLAGS2_CODE_VAL {
    pub auto_is_null: bool,
    pub auto_commit: bool,
    pub foreign_key_checks: bool,
    pub unique_checks: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Q_SQL_MODE_CODE_VAL {
    pub real_as_float: bool,
    pub pipes_as_concat: bool,
    pub ansi_quotes: bool,
    pub ignore_space: bool,
    pub not_used: bool,
    pub only_full_group_by: bool,
    pub no_unsigned_subtraction: bool,
    pub no_dir_in_create: bool,
    pub postgresql: bool,
    pub oracle: bool,
    pub mssql: bool,
    pub db2: bool,
    pub maxdb: bool,
    pub no_key_options: bool,
    pub no_table_options: bool,
    pub no_field_options: bool,
    pub mysql323: bool,
    pub mysql40: bool,
    pub ansi: bool,
    pub no_auto_value_on_zero: bool,
    pub no_backslash_escapes: bool,
    pub strict_trans_tables: bool,
    pub strict_all_tables: bool,
    pub no_zero_in_date: bool,
    pub no_zero_date: bool,
    pub invalid_dates: bool,
    pub error_for_division_by_zero: bool,
    pub traditional: bool,
    pub no_auto_create_user: bool,
    pub high_not_precedence: bool,
    pub no_engine_substitution: bool,
    pub pad_char_to_full_length: bool,
}

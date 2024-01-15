use crate::column::column_type::ColumnType;
use crate::dump::dump_table_map_event::DumpTableMapEvent;
use crate::dump::dump_write_rows_event::DumpWriteRowsEvent;
use crate::metadata::table_metadata::TableMetadata;
use crate::row::row_data::RowData;

/// 构造 DumpTableMapEvent
pub fn build_dump_table_map_event(database_name: String, table_name: String, columns_number: u64, column_metadata_type: Vec<ColumnType>,
                                  null_bitmap: Vec<u8>, table_metadata: TableMetadata) -> DumpTableMapEvent {

    DumpTableMapEvent::new(
        database_name, table_name,
        columns_number, column_metadata_type,
        null_bitmap,
        Some(table_metadata)
    )
}

/// 从 dump 数据中构造 DumpWriteRowsEvent
pub fn build_write_rows_event (
    columns_number: usize,
    table_metadata: TableMetadata,
    rows: Vec<RowData>) -> DumpWriteRowsEvent {

    DumpWriteRowsEvent::new(
        columns_number,
        Some(table_metadata),
        rows,
    )
}

/// 从 dump 数据中构造 TableMetadata
pub fn build_table_metadata_assembly_dump_row (
    column_charsets: Option<Vec<u32>>,
    column_names: Option<Vec<String>>,
    set_string_values: Option<Vec<Vec<String>>>) -> TableMetadata {

    TableMetadata::new(
        None,
        None,
        column_charsets,
        column_names,
        set_string_values,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}
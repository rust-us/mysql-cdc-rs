use common::binlog::column::column_type::SrcColumnType;
use crate::dump::dump_table_map_event::DumpTableMapEvent;
use crate::dump::dump_write_rows_event::DumpWriteRowsEvent;
use crate::row::row_data::RowData;

/// 构造 DumpTableMapEvent
pub fn build_dump_table_map_event(database_name: String, table_name: String, columns_number: u64, column_metadata_type: Vec<SrcColumnType>,
                                  null_bitmap: Vec<u8>, default_charset: u32) -> DumpTableMapEvent {

    DumpTableMapEvent::new(
        database_name, table_name,
        columns_number, column_metadata_type,
        null_bitmap,
        default_charset
    )
}

/// 从 dump 数据中构造 DumpWriteRowsEvent
pub fn build_write_rows_event (
    columns_number: usize,
    rows: Vec<RowData>) -> DumpWriteRowsEvent {

    DumpWriteRowsEvent::new(
        columns_number,
        rows,
    )
}
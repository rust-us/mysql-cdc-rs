use serde::Serialize;
use crate::metadata::table_metadata::TableMetadata;
use crate::row::row_data::RowData;

#[derive(Debug, Serialize, Clone)]
pub struct DumpWriteRowsEvent {
    /// Gets number of columns in the table, Bitmap denoting columns available.
    pub columns_number: usize,

    /// dump row field: cloumn
    pub table_metadata: Option<TableMetadata>,

    /// dump row field: value
    pub rows: Vec<RowData>,

}

impl DumpWriteRowsEvent {
    pub fn new(columns_number: usize, table_metadata: Option<TableMetadata>, rows: Vec<RowData>) -> Self {
        DumpWriteRowsEvent {
            columns_number,
            table_metadata,
            rows,
        }
    }
}
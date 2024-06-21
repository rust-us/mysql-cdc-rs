use serde::Serialize;
use crate::row::row_data::RowData;

#[derive(Debug, Serialize, Clone)]
pub struct DumpWriteRowsEvent {
    /// Gets number of columns in the table, Bitmap denoting columns available.
    pub columns_number: usize,

    /// dump row field: value
    pub rows: Vec<RowData>,
}

impl DumpWriteRowsEvent {
    pub fn new(columns_number: usize, rows: Vec<RowData>) -> Self {
        DumpWriteRowsEvent {
            columns_number,
            rows,
        }
    }
}
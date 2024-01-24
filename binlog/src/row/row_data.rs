use serde::Serialize;
use crate::column::column_value::ColumnValue;

/// Represents an inserted or deleted row in row based replication.
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct RowData {
    /// Column values of the changed row.
    /// 该列存在值则为 Some(xx)， 不存在之则为None 即可
    pub cells: Vec<Option<ColumnValue>>,
}

impl Default for RowData {
    fn default() -> Self {
        RowData::new()
    }
}

impl RowData {
    pub fn new() -> Self {
        Self {
            cells: Vec::new()
        }
    }

    pub fn new_with_cells(cells: Vec<Option<ColumnValue>>) -> Self {
        Self { cells }
    }
}

impl RowData {
    pub fn insert(&mut self, index: usize, cell: Option<ColumnValue>) {
        self.cells.insert(index, cell);
    }

    pub fn push(&mut self, cell: Option<ColumnValue>) {
        self.cells.push(cell);
    }
}

/// Represents an updated row in row based replication.
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct UpdateRowData {
    /// Row state before it was updated.
    pub before_update: RowData,

    /// Actual row state after update.
    pub after_update: RowData,
}

impl UpdateRowData {
    pub fn new(before_update: RowData, after_update: RowData) -> Self {
        Self {
            before_update,
            after_update,
        }
    }
}

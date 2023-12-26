use serde::Serialize;
use crate::column::column_value::ColumnValue;

/// Represents an inserted or deleted row in row based replication.
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct RowData {
    /// Column values of the changed row.
    pub cells: Vec<Option<ColumnValue>>,
}

impl RowData {
    pub fn new(cells: Vec<Option<ColumnValue>>) -> Self {
        Self { cells }
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

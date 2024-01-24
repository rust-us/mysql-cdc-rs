use std::fmt;
use std::sync::Arc;
use crate::column::column_value::ColumnValue;
use crate::column::column::Column;

#[derive(Clone, PartialEq)]
pub struct Row {
    values: Vec<Option<ColumnValue>>,

    columns: Arc<[Column]>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Row");
        for (val, column) in self.values.iter().zip(self.columns.iter()) {
            match *val {
                Some(ref val) => {
                    debug.field(column.name_str().as_ref(), val);
                }
                None => {
                    debug.field(column.name_str().as_ref(), &"<taken>");
                }
            }
        }
        debug.finish()
    }
}

impl Row {

    /// Creates `Row` from values and columns.
    pub fn new_row(values: Vec<ColumnValue>, columns: Arc<[Column]>) -> Self {
        assert_eq!(values.len(), columns.len());

        Row {
            values: values.into_iter().map(Some).collect::<Vec<_>>(),
            columns,
        }
    }

    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns columns of this row.
    pub fn columns_ref(&self) -> &[Column] {
        &*self.columns
    }

    /// Returns columns of this row.
    pub fn columns(&self) -> Arc<[Column]> {
        self.columns.clone()
    }
}
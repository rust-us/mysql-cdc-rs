use std::fmt;
use std::sync::Arc;
use crate::binlog::column::column::SrcColumn;
use crate::binlog::column::column_value::SrcColumnValue;

#[derive(Clone, PartialEq)]
pub struct Row {
    values: Vec<Option<SrcColumnValue>>,

    columns: Arc<[SrcColumn]>,
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
    pub fn new_row(values: Vec<Option<SrcColumnValue>>, columns: Arc<[SrcColumn]>) -> Self {
        assert_eq!(values.len(), columns.len());

        Row {
            values,
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
    pub fn columns_ref(&self) -> &[SrcColumn] {
        &*self.columns
    }

    /// Returns columns of this row.
    pub fn columns(&self) -> Arc<[SrcColumn]> {
        self.columns.clone()
    }
}
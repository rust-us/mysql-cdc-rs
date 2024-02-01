use serde::Serialize;
use common::binlog::column::column_value::SrcColumnValue;

/// Represents an inserted or deleted row in row based replication.
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct RowData {
    /// Column values of the changed row.
    /// 该列存在值则为 Some(xx)， 不存在之则为None 即可
    pub cells: Vec<Option<SrcColumnValue>>,
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

    pub fn new_with_cells(cells: Vec<Option<SrcColumnValue>>) -> Self {
        Self { cells }
    }

    pub fn get_cells(&self) -> &[Option<SrcColumnValue>] {
        self.cells.as_slice()
    }
}

impl RowData {
    pub fn insert(&mut self, index: usize, cell: Option<SrcColumnValue>) {
        self.cells.insert(index, cell);
    }

    pub fn push(&mut self, cell: Option<SrcColumnValue>) {
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

    pub fn get_before_update(&self) -> RowData {
        self.before_update.clone()
    }

    pub fn get_after_update(&self) -> RowData {
        self.after_update.clone()
    }
}

#[cfg(test)]
mod tests {
    use common::binlog::column::column_value::SrcColumnValue;
    use common::binlog::column::column_value::SrcColumnValue::BigInt;
    use crate::row::row_data::RowData;

    #[test]
    fn test_row_data() {
        let len = 6usize;

        let mut cells = Vec::<Option<SrcColumnValue>>::new();
        for i in 0..len {
            cells.push(Some(BigInt((i * 1000) as u64)));
        }

        let row = RowData {
            cells,
        };

        let get_cells = row.get_cells();
        assert_eq!(&get_cells.len(), &len);

        for i in 0..len {
            let cell = &get_cells[i];
            assert_eq!(cell.as_ref().unwrap(), &BigInt((i * 1000) as u64));
        }

        for i in 0..8 {
            if i >= len {
                // index out of bounds
            } else {
                let cell = &get_cells[i];
                assert!(cell.is_some());
                assert_eq!(cell.as_ref().unwrap(), &BigInt((i * 1000) as u64));
            }
        }
    }
}


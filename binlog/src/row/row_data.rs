use serde::Serialize;
use std::collections::HashMap;
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

/// Represents a field-level change in an update operation
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct FieldChange {
    /// Column index
    pub column_index: usize,
    /// Value before update
    pub before_value: Option<SrcColumnValue>,
    /// Value after update
    pub after_value: Option<SrcColumnValue>,
}

impl FieldChange {
    pub fn new(column_index: usize, before_value: Option<SrcColumnValue>, after_value: Option<SrcColumnValue>) -> Self {
        Self {
            column_index,
            before_value,
            after_value,
        }
    }

    /// Check if this represents a null-to-value change
    pub fn is_null_to_value(&self) -> bool {
        self.before_value.is_none() && self.after_value.is_some()
    }

    /// Check if this represents a value-to-null change
    pub fn is_value_to_null(&self) -> bool {
        self.before_value.is_some() && self.after_value.is_none()
    }

    /// Check if this represents a value-to-value change
    pub fn is_value_change(&self) -> bool {
        self.before_value.is_some() && self.after_value.is_some()
    }
}

/// Represents the difference between before and after states of an update
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct UpdateDifference {
    /// List of changed fields with their before/after values
    pub changed_fields: Vec<FieldChange>,
    /// Map of column index to field change for fast lookup
    pub change_map: HashMap<usize, FieldChange>,
    /// Total number of columns in the row
    pub total_columns: usize,
    /// Number of changed columns
    pub changed_count: usize,
}

impl UpdateDifference {
    pub fn new(total_columns: usize) -> Self {
        Self {
            changed_fields: Vec::new(),
            change_map: HashMap::new(),
            total_columns,
            changed_count: 0,
        }
    }

    /// Add a field change to the difference
    pub fn add_change(&mut self, change: FieldChange) {
        self.change_map.insert(change.column_index, change.clone());
        self.changed_fields.push(change);
        self.changed_count += 1;
    }

    /// Check if a specific column was changed
    pub fn is_column_changed(&self, column_index: usize) -> bool {
        self.change_map.contains_key(&column_index)
    }

    /// Get the change for a specific column
    pub fn get_column_change(&self, column_index: usize) -> Option<&FieldChange> {
        self.change_map.get(&column_index)
    }

    /// Get the percentage of columns that changed
    pub fn change_percentage(&self) -> f64 {
        if self.total_columns > 0 {
            (self.changed_count as f64 / self.total_columns as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Check if this is a partial update (not all columns changed)
    pub fn is_partial_update(&self) -> bool {
        self.changed_count < self.total_columns && self.changed_count > 0
    }

    /// Get only the changed column values as a sparse representation
    pub fn get_changed_values_only(&self) -> HashMap<usize, (Option<SrcColumnValue>, Option<SrcColumnValue>)> {
        self.change_map.iter()
            .map(|(idx, change)| (*idx, (change.before_value.clone(), change.after_value.clone())))
            .collect()
    }
}

/// Enhanced UpdateRowData with field-level difference detection
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct UpdateRowData {
    /// Row state before it was updated.
    pub before_update: RowData,

    /// Actual row state after update.
    pub after_update: RowData,

    /// Computed difference between before and after states
    pub difference: Option<UpdateDifference>,

    /// Whether difference detection is enabled
    pub enable_difference_detection: bool,
}

impl UpdateRowData {
    pub fn new(before_update: RowData, after_update: RowData) -> Self {
        Self {
            before_update,
            after_update,
            difference: None,
            enable_difference_detection: false,
        }
    }

    /// Create UpdateRowData with difference detection enabled
    pub fn new_with_difference_detection(before_update: RowData, after_update: RowData) -> Self {
        let mut update_data = Self {
            before_update,
            after_update,
            difference: None,
            enable_difference_detection: true,
        };
        update_data.compute_difference();
        update_data
    }

    /// Create UpdateRowData for partial column updates
    pub fn new_partial_update(
        before_update: RowData,
        after_update: RowData,
        changed_columns: &[usize],
    ) -> Self {
        let mut update_data = Self::new_with_difference_detection(before_update, after_update);
        
        // Filter difference to only include specified columns
        if let Some(ref mut diff) = update_data.difference {
            diff.changed_fields.retain(|change| changed_columns.contains(&change.column_index));
            diff.change_map.retain(|idx, _| changed_columns.contains(idx));
            diff.changed_count = diff.changed_fields.len();
        }
        
        update_data
    }

    /// Compute the difference between before and after states
    pub fn compute_difference(&mut self) {
        let before_cells = &self.before_update.cells;
        let after_cells = &self.after_update.cells;
        
        let max_len = before_cells.len().max(after_cells.len());
        let mut difference = UpdateDifference::new(max_len);

        for i in 0..max_len {
            let before_value = before_cells.get(i).cloned().flatten();
            let after_value = after_cells.get(i).cloned().flatten();

            // Check if values are different
            if before_value != after_value {
                let change = FieldChange::new(i, before_value, after_value);
                difference.add_change(change);
            }
        }

        self.difference = Some(difference);
    }

    /// Get the computed difference, computing it if not already done
    pub fn get_difference(&mut self) -> &UpdateDifference {
        if self.difference.is_none() {
            self.compute_difference();
        }
        self.difference.as_ref().unwrap()
    }

    /// Get the difference without computing it (returns None if not computed)
    pub fn get_difference_readonly(&self) -> Option<&UpdateDifference> {
        self.difference.as_ref()
    }

    /// Check if any fields were changed
    pub fn has_changes(&mut self) -> bool {
        let diff = self.get_difference();
        diff.changed_count > 0
    }

    /// Get only the changed fields as a compact representation
    pub fn get_changed_fields_only(&mut self) -> &[FieldChange] {
        let diff = self.get_difference();
        &diff.changed_fields
    }

    /// Create a memory-optimized representation with only changed data
    pub fn to_incremental_update(&mut self) -> IncrementalUpdate {
        let diff = self.get_difference();
        IncrementalUpdate {
            changed_values: diff.get_changed_values_only(),
            total_columns: diff.total_columns,
            change_percentage: diff.change_percentage(),
        }
    }

    pub fn get_before_update(&self) -> RowData {
        self.before_update.clone()
    }

    pub fn get_after_update(&self) -> RowData {
        self.after_update.clone()
    }

    /// Enable or disable difference detection
    pub fn set_difference_detection(&mut self, enabled: bool) {
        self.enable_difference_detection = enabled;
        if enabled && self.difference.is_none() {
            self.compute_difference();
        } else if !enabled {
            self.difference = None;
        }
    }

    /// Get memory usage statistics for this update
    pub fn get_memory_stats(&self) -> UpdateMemoryStats {
        let before_size = self.before_update.cells.len() * std::mem::size_of::<Option<SrcColumnValue>>();
        let after_size = self.after_update.cells.len() * std::mem::size_of::<Option<SrcColumnValue>>();
        let diff_size = if let Some(ref diff) = self.difference {
            diff.changed_fields.len() * std::mem::size_of::<FieldChange>() +
            diff.change_map.len() * (std::mem::size_of::<usize>() + std::mem::size_of::<FieldChange>())
        } else {
            0
        };

        UpdateMemoryStats {
            before_row_size: before_size,
            after_row_size: after_size,
            difference_size: diff_size,
            total_size: before_size + after_size + diff_size,
        }
    }
}

/// Memory-optimized incremental update representation
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct IncrementalUpdate {
    /// Only the changed column values (column_index -> (before, after))
    pub changed_values: HashMap<usize, (Option<SrcColumnValue>, Option<SrcColumnValue>)>,
    /// Total number of columns in the original row
    pub total_columns: usize,
    /// Percentage of columns that changed
    pub change_percentage: f64,
}

impl IncrementalUpdate {
    /// Get the number of changed columns
    pub fn changed_count(&self) -> usize {
        self.changed_values.len()
    }

    /// Check if this is a sparse update (few columns changed)
    pub fn is_sparse_update(&self, threshold_percentage: f64) -> bool {
        self.change_percentage < threshold_percentage
    }

    /// Get memory savings compared to full row storage
    pub fn memory_savings_percentage(&self) -> f64 {
        if self.total_columns > 0 {
            let full_size = self.total_columns * 2; // before + after for all columns
            let sparse_size = self.changed_values.len() * 2; // before + after for changed columns only
            ((full_size - sparse_size) as f64 / full_size as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Memory usage statistics for update operations
#[derive(Debug, Clone)]
pub struct UpdateMemoryStats {
    pub before_row_size: usize,
    pub after_row_size: usize,
    pub difference_size: usize,
    pub total_size: usize,
}

impl UpdateMemoryStats {
    /// Calculate memory overhead of difference detection
    pub fn difference_overhead_percentage(&self) -> f64 {
        let base_size = self.before_row_size + self.after_row_size;
        if base_size > 0 {
            (self.difference_size as f64 / base_size as f64) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::binlog::column::column_value::SrcColumnValue;
    use common::binlog::column::column_value::SrcColumnValue::{BigInt, Int, String as SrcString};

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

    #[test]
    fn test_field_change() {
        let change = FieldChange::new(0, Some(Int(10)), Some(Int(20)));
        assert_eq!(change.column_index, 0);
        assert!(!change.is_null_to_value());
        assert!(!change.is_value_to_null());
        assert!(change.is_value_change());

        let null_to_value = FieldChange::new(1, None, Some(Int(30)));
        assert!(null_to_value.is_null_to_value());
        assert!(!null_to_value.is_value_to_null());
        assert!(!null_to_value.is_value_change());

        let value_to_null = FieldChange::new(2, Some(Int(40)), None);
        assert!(!value_to_null.is_null_to_value());
        assert!(value_to_null.is_value_to_null());
        assert!(!value_to_null.is_value_change());
    }

    #[test]
    fn test_update_difference() {
        let mut diff = UpdateDifference::new(5);
        assert_eq!(diff.total_columns, 5);
        assert_eq!(diff.changed_count, 0);
        assert_eq!(diff.change_percentage(), 0.0);

        let change1 = FieldChange::new(0, Some(Int(10)), Some(Int(20)));
        let change2 = FieldChange::new(2, None, Some(Int(30)));
        
        diff.add_change(change1);
        diff.add_change(change2);

        assert_eq!(diff.changed_count, 2);
        assert_eq!(diff.change_percentage(), 40.0);
        assert!(diff.is_partial_update());
        assert!(diff.is_column_changed(0));
        assert!(!diff.is_column_changed(1));
        assert!(diff.is_column_changed(2));

        let change = diff.get_column_change(0).unwrap();
        assert_eq!(change.column_index, 0);
    }

    #[test]
    fn test_update_row_data_basic() {
        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("hello".to_string())),
            Some(Int(3)),
        ]);
        
        let after = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("world".to_string())),
            Some(Int(4)),
        ]);

        let update = UpdateRowData::new(before, after);
        assert!(!update.enable_difference_detection);
        assert!(update.difference.is_none());
    }

    #[test]
    fn test_update_row_data_with_difference_detection() {
        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("hello".to_string())),
            Some(Int(3)),
            None,
        ]);
        
        let after = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("world".to_string())),
            Some(Int(4)),
            Some(Int(5)),
        ]);

        let mut update = UpdateRowData::new_with_difference_detection(before, after);
        assert!(update.enable_difference_detection);
        assert!(update.difference.is_some());

        let diff = update.get_difference();
        assert_eq!(diff.changed_count, 3); // columns 1, 2, 3 changed
        assert_eq!(diff.total_columns, 4);
        assert_eq!(diff.change_percentage(), 75.0);
        assert!(diff.is_partial_update());

        assert!(!diff.is_column_changed(0)); // column 0 unchanged
        assert!(diff.is_column_changed(1));  // column 1 changed
        assert!(diff.is_column_changed(2));  // column 2 changed
        assert!(diff.is_column_changed(3));  // column 3 changed (null to value)

        let change3 = diff.get_column_change(3).unwrap();
        assert!(change3.is_null_to_value());
    }

    #[test]
    fn test_partial_update() {
        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("hello".to_string())),
            Some(Int(3)),
            Some(Int(4)),
        ]);
        
        let after = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("world".to_string())),
            Some(Int(5)),
            Some(Int(4)),
        ]);

        let changed_columns = vec![1, 2]; // Only columns 1 and 2 should be considered
        let update = UpdateRowData::new_partial_update(before, after, &changed_columns);
        
        let diff = update.get_difference_readonly().unwrap();
        assert_eq!(diff.changed_count, 2);
        assert!(diff.is_column_changed(1));
        assert!(diff.is_column_changed(2));
        assert!(!diff.is_column_changed(0)); // Not in changed_columns, so filtered out
        assert!(!diff.is_column_changed(3)); // Not in changed_columns, so filtered out
    }

    #[test]
    fn test_incremental_update() {
        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("hello".to_string())),
            Some(Int(3)),
            Some(Int(4)),
            Some(Int(5)),
        ]);
        
        let after = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("world".to_string())),
            Some(Int(3)),
            Some(Int(4)),
            Some(Int(6)),
        ]);

        let mut update = UpdateRowData::new_with_difference_detection(before, after);
        let incremental = update.to_incremental_update();

        assert_eq!(incremental.total_columns, 5);
        assert_eq!(incremental.changed_count(), 2); // columns 1 and 4 changed
        assert_eq!(incremental.change_percentage, 40.0);
        assert!(incremental.is_sparse_update(50.0));
        
        let memory_savings = incremental.memory_savings_percentage();
        assert!(memory_savings > 0.0);
        assert_eq!(memory_savings, 60.0); // (10 - 4) / 10 * 100 = 60%
    }

    #[test]
    fn test_memory_stats() {
        let before = RowData::new_with_cells(vec![Some(Int(1)), Some(Int(2))]);
        let after = RowData::new_with_cells(vec![Some(Int(1)), Some(Int(3))]);
        
        let update = UpdateRowData::new_with_difference_detection(before, after);
        let stats = update.get_memory_stats();
        
        assert!(stats.before_row_size > 0);
        assert!(stats.after_row_size > 0);
        assert!(stats.difference_size > 0);
        assert_eq!(stats.total_size, stats.before_row_size + stats.after_row_size + stats.difference_size);
        
        let overhead = stats.difference_overhead_percentage();
        assert!(overhead > 0.0);
    }

    #[test]
    fn test_update_row_data_no_changes() {
        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("hello".to_string())),
            Some(Int(3)),
        ]);
        
        let after = before.clone();

        let mut update = UpdateRowData::new_with_difference_detection(before, after);
        assert!(!update.has_changes());
        
        let diff = update.get_difference();
        assert_eq!(diff.changed_count, 0);
        assert_eq!(diff.change_percentage(), 0.0);
        assert!(!diff.is_partial_update());
    }

    #[test]
    fn test_difference_detection_toggle() {
        let before = RowData::new_with_cells(vec![Some(Int(1))]);
        let after = RowData::new_with_cells(vec![Some(Int(2))]);
        
        let mut update = UpdateRowData::new(before, after);
        assert!(!update.enable_difference_detection);
        assert!(update.difference.is_none());
        
        // Enable difference detection
        update.set_difference_detection(true);
        assert!(update.enable_difference_detection);
        assert!(update.difference.is_some());
        
        // Disable difference detection
        update.set_difference_detection(false);
        assert!(!update.enable_difference_detection);
        assert!(update.difference.is_none());
    }
}


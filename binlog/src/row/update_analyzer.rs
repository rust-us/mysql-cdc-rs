use std::collections::HashMap;
use common::err::decode_error::ReError;
use crate::row::row_data::{UpdateRowData, UpdateDifference, FieldChange, IncrementalUpdate};
use crate::events::protocol::table_map_event::TableMapEvent;

/// Analyzer for update row data with advanced difference detection capabilities
#[derive(Debug)]
pub struct UpdateAnalyzer {
    /// Configuration for analysis behavior
    config: UpdateAnalysisConfig,
    /// Statistics for analysis operations
    stats: UpdateAnalysisStats,
}

/// Configuration for update analysis
#[derive(Debug, Clone)]
pub struct UpdateAnalysisConfig {
    /// Enable field-level difference detection
    pub enable_difference_detection: bool,
    /// Threshold for considering an update as sparse (percentage)
    pub sparse_update_threshold: f64,
    /// Maximum memory overhead allowed for difference detection (percentage)
    pub max_memory_overhead: f64,
    /// Enable incremental update optimization
    pub enable_incremental_optimization: bool,
    /// Columns to ignore in difference detection
    pub ignored_columns: Vec<usize>,
}

impl Default for UpdateAnalysisConfig {
    fn default() -> Self {
        Self {
            enable_difference_detection: true,
            sparse_update_threshold: 30.0, // Consider updates with <30% changed columns as sparse
            max_memory_overhead: 50.0,     // Allow up to 50% memory overhead for difference detection
            enable_incremental_optimization: true,
            ignored_columns: Vec::new(),
        }
    }
}

/// Statistics for update analysis operations
#[derive(Debug, Default, Clone)]
pub struct UpdateAnalysisStats {
    pub total_updates_analyzed: u64,
    pub sparse_updates_detected: u64,
    pub full_updates_detected: u64,
    pub memory_optimizations_applied: u64,
    pub total_fields_analyzed: u64,
    pub changed_fields_detected: u64,
    pub analysis_time_ns: u64,
}

impl UpdateAnalysisStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_analysis(&mut self, changed_fields: usize, total_fields: usize, is_sparse: bool, analysis_time_ns: u64) {
        self.total_updates_analyzed += 1;
        self.total_fields_analyzed += total_fields as u64;
        self.changed_fields_detected += changed_fields as u64;
        self.analysis_time_ns += analysis_time_ns;
        
        if is_sparse {
            self.sparse_updates_detected += 1;
        } else {
            self.full_updates_detected += 1;
        }
    }

    pub fn add_memory_optimization(&mut self) {
        self.memory_optimizations_applied += 1;
    }

    pub fn average_analysis_time_ns(&self) -> f64 {
        if self.total_updates_analyzed > 0 {
            self.analysis_time_ns as f64 / self.total_updates_analyzed as f64
        } else {
            0.0
        }
    }

    pub fn change_detection_ratio(&self) -> f64 {
        if self.total_fields_analyzed > 0 {
            self.changed_fields_detected as f64 / self.total_fields_analyzed as f64
        } else {
            0.0
        }
    }

    pub fn sparse_update_ratio(&self) -> f64 {
        if self.total_updates_analyzed > 0 {
            self.sparse_updates_detected as f64 / self.total_updates_analyzed as f64
        } else {
            0.0
        }
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl UpdateAnalyzer {
    pub fn new(config: UpdateAnalysisConfig) -> Self {
        Self {
            config,
            stats: UpdateAnalysisStats::new(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(UpdateAnalysisConfig::default())
    }

    /// Analyze a batch of update row data for optimization opportunities
    pub fn analyze_update_batch(
        &mut self,
        updates: &mut [UpdateRowData],
        table_map: &TableMapEvent,
    ) -> Result<UpdateBatchAnalysis, ReError> {
        let start_time = std::time::Instant::now();
        let mut batch_analysis = UpdateBatchAnalysis::new(updates.len());

        for update in updates.iter_mut() {
            let analysis = self.analyze_single_update(update, table_map)?;
            batch_analysis.add_update_analysis(analysis);
        }

        let analysis_time = start_time.elapsed().as_nanos() as u64;
        batch_analysis.total_analysis_time_ns = analysis_time;

        Ok(batch_analysis)
    }

    /// Analyze a single update for difference detection and optimization
    pub fn analyze_single_update(
        &mut self,
        update: &mut UpdateRowData,
        table_map: &TableMapEvent,
    ) -> Result<SingleUpdateAnalysis, ReError> {
        let start_time = std::time::Instant::now();

        // Enable difference detection if configured
        if self.config.enable_difference_detection && !update.enable_difference_detection {
            update.set_difference_detection(true);
        }

        let memory_stats = update.get_memory_stats();
        let difference = update.get_difference();

        // Check if memory overhead is acceptable
        let memory_overhead = memory_stats.difference_overhead_percentage();
        let memory_acceptable = memory_overhead <= self.config.max_memory_overhead;

        // Determine if this is a sparse update
        let is_sparse = difference.change_percentage() < self.config.sparse_update_threshold;

        // Filter out ignored columns from analysis
        let filtered_changes = self.filter_ignored_columns(&difference.changed_fields);
        let effective_change_count = filtered_changes.len();
        let effective_change_percentage = if difference.total_columns > 0 {
            (effective_change_count as f64 / difference.total_columns as f64) * 100.0
        } else {
            0.0
        };

        let analysis_time = start_time.elapsed().as_nanos() as u64;

        // Update statistics
        self.stats.add_analysis(
            effective_change_count,
            difference.total_columns,
            is_sparse,
            analysis_time,
        );

        if memory_acceptable && self.config.enable_incremental_optimization {
            self.stats.add_memory_optimization();
        }

        Ok(SingleUpdateAnalysis {
            original_change_count: difference.changed_count,
            filtered_change_count: effective_change_count,
            total_columns: difference.total_columns,
            change_percentage: difference.change_percentage(),
            effective_change_percentage,
            is_sparse_update: is_sparse,
            memory_overhead_percentage: memory_overhead,
            memory_acceptable,
            optimization_recommended: memory_acceptable && is_sparse,
            filtered_changes,
            analysis_time_ns: analysis_time,
        })
    }

    /// Convert updates to incremental format for memory optimization
    pub fn optimize_updates_to_incremental(
        &mut self,
        updates: &mut [UpdateRowData],
        table_map: &TableMapEvent,
    ) -> Result<Vec<IncrementalUpdate>, ReError> {
        let mut incremental_updates = Vec::with_capacity(updates.len());

        for update in updates.iter_mut() {
            let analysis = self.analyze_single_update(update, table_map)?;
            
            if analysis.optimization_recommended {
                let incremental = update.to_incremental_update();
                incremental_updates.push(incremental);
            } else {
                // For non-optimizable updates, still create incremental but mark as full
                let incremental = update.to_incremental_update();
                incremental_updates.push(incremental);
            }
        }

        Ok(incremental_updates)
    }

    /// Detect partial column updates based on column patterns
    pub fn detect_partial_column_patterns(
        &self,
        updates: &[UpdateRowData],
    ) -> Result<PartialColumnPattern, ReError> {
        let mut column_change_frequency = HashMap::new();
        let mut total_updates = 0;

        for update in updates {
            if let Some(diff) = update.get_difference_readonly() {
                total_updates += 1;
                for change in &diff.changed_fields {
                    if !self.config.ignored_columns.contains(&change.column_index) {
                        *column_change_frequency.entry(change.column_index).or_insert(0) += 1;
                    }
                }
            }
        }

        // Identify frequently changed columns
        let mut frequent_columns = Vec::new();
        let mut rare_columns = Vec::new();
        let frequency_threshold = (total_updates as f64 * 0.5) as usize; // 50% threshold

        for (column_index, frequency) in &column_change_frequency {
            if *frequency >= frequency_threshold {
                frequent_columns.push(*column_index);
            } else {
                rare_columns.push(*column_index);
            }
        }

        Ok(PartialColumnPattern {
            total_updates_analyzed: total_updates,
            frequently_changed_columns: frequent_columns,
            rarely_changed_columns: rare_columns,
            column_change_frequency,
        })
    }

    /// Filter out ignored columns from field changes
    fn filter_ignored_columns(&self, changes: &[FieldChange]) -> Vec<FieldChange> {
        changes
            .iter()
            .filter(|change| !self.config.ignored_columns.contains(&change.column_index))
            .cloned()
            .collect()
    }

    /// Get current configuration
    pub fn get_config(&self) -> &UpdateAnalysisConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: UpdateAnalysisConfig) {
        self.config = config;
    }

    /// Get analysis statistics
    pub fn get_stats(&self) -> &UpdateAnalysisStats {
        &self.stats
    }

    /// Reset analysis statistics
    pub fn reset_stats(&mut self) {
        self.stats.reset();
    }
}

/// Analysis result for a single update
#[derive(Debug, Clone)]
pub struct SingleUpdateAnalysis {
    pub original_change_count: usize,
    pub filtered_change_count: usize,
    pub total_columns: usize,
    pub change_percentage: f64,
    pub effective_change_percentage: f64,
    pub is_sparse_update: bool,
    pub memory_overhead_percentage: f64,
    pub memory_acceptable: bool,
    pub optimization_recommended: bool,
    pub filtered_changes: Vec<FieldChange>,
    pub analysis_time_ns: u64,
}

/// Analysis result for a batch of updates
#[derive(Debug)]
pub struct UpdateBatchAnalysis {
    pub total_updates: usize,
    pub sparse_updates: usize,
    pub optimizable_updates: usize,
    pub total_memory_overhead: f64,
    pub average_change_percentage: f64,
    pub total_analysis_time_ns: u64,
    pub update_analyses: Vec<SingleUpdateAnalysis>,
}

impl UpdateBatchAnalysis {
    pub fn new(capacity: usize) -> Self {
        Self {
            total_updates: 0,
            sparse_updates: 0,
            optimizable_updates: 0,
            total_memory_overhead: 0.0,
            average_change_percentage: 0.0,
            total_analysis_time_ns: 0,
            update_analyses: Vec::with_capacity(capacity),
        }
    }

    pub fn add_update_analysis(&mut self, analysis: SingleUpdateAnalysis) {
        self.total_updates += 1;
        
        if analysis.is_sparse_update {
            self.sparse_updates += 1;
        }
        
        if analysis.optimization_recommended {
            self.optimizable_updates += 1;
        }
        
        self.total_memory_overhead += analysis.memory_overhead_percentage;
        self.average_change_percentage += analysis.change_percentage;
        
        self.update_analyses.push(analysis);
    }

    pub fn finalize(&mut self) {
        if self.total_updates > 0 {
            self.average_change_percentage /= self.total_updates as f64;
            self.total_memory_overhead /= self.total_updates as f64;
        }
    }

    pub fn optimization_potential(&self) -> f64 {
        if self.total_updates > 0 {
            (self.optimizable_updates as f64 / self.total_updates as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Pattern analysis for partial column updates
#[derive(Debug)]
pub struct PartialColumnPattern {
    pub total_updates_analyzed: usize,
    pub frequently_changed_columns: Vec<usize>,
    pub rarely_changed_columns: Vec<usize>,
    pub column_change_frequency: HashMap<usize, usize>,
}

impl PartialColumnPattern {
    /// Get columns that change more than the specified percentage of the time
    pub fn get_columns_above_frequency(&self, frequency_percentage: f64) -> Vec<usize> {
        let threshold = (self.total_updates_analyzed as f64 * frequency_percentage / 100.0) as usize;
        self.column_change_frequency
            .iter()
            .filter(|(_, &freq)| freq >= threshold)
            .map(|(&col_idx, _)| col_idx)
            .collect()
    }

    /// Get the change frequency for a specific column
    pub fn get_column_frequency(&self, column_index: usize) -> f64 {
        if self.total_updates_analyzed > 0 {
            let frequency = self.column_change_frequency.get(&column_index).unwrap_or(&0);
            (*frequency as f64 / self.total_updates_analyzed as f64) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::row_data::RowData;
    use common::binlog::column::column_value::SrcColumnValue::{Int, String as SrcString};

    #[test]
    fn test_update_analyzer_creation() {
        let analyzer = UpdateAnalyzer::with_default_config();
        assert!(analyzer.config.enable_difference_detection);
        assert_eq!(analyzer.config.sparse_update_threshold, 30.0);
    }

    #[test]
    fn test_single_update_analysis() {
        let mut analyzer = UpdateAnalyzer::with_default_config();
        let table_map = TableMapEvent::default();

        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("hello".to_string())),
            Some(Int(3)),
            Some(Int(4)),
        ]);
        
        let after = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(SrcString("world".to_string())),
            Some(Int(3)),
            Some(Int(4)),
        ]);

        let mut update = UpdateRowData::new(before, after);
        let analysis = analyzer.analyze_single_update(&mut update, &table_map).unwrap();

        assert_eq!(analysis.total_columns, 4);
        assert_eq!(analysis.original_change_count, 1);
        assert_eq!(analysis.filtered_change_count, 1);
        assert_eq!(analysis.change_percentage, 25.0);
        assert!(analysis.is_sparse_update); // 25% < 30% threshold
    }

    #[test]
    fn test_partial_column_pattern_detection() {
        let analyzer = UpdateAnalyzer::with_default_config();

        let mut updates = Vec::new();
        
        // Create updates where column 1 changes frequently
        for i in 0..10 {
            let before = RowData::new_with_cells(vec![
                Some(Int(1)),
                Some(Int(i)),
                Some(Int(3)),
            ]);
            
            let after = RowData::new_with_cells(vec![
                Some(Int(1)),
                Some(Int(i + 100)),
                Some(Int(3)),
            ]);
            
            let update = UpdateRowData::new_with_difference_detection(before, after);
            updates.push(update);
        }

        let pattern = analyzer.detect_partial_column_patterns(&updates).unwrap();
        assert_eq!(pattern.total_updates_analyzed, 10);
        assert!(pattern.frequently_changed_columns.contains(&1));
        assert_eq!(pattern.get_column_frequency(1), 100.0); // Column 1 changes in all updates
        assert_eq!(pattern.get_column_frequency(0), 0.0);   // Column 0 never changes
    }

    #[test]
    fn test_update_analysis_stats() {
        let mut stats = UpdateAnalysisStats::new();
        
        stats.add_analysis(2, 10, true, 1000);
        stats.add_analysis(8, 10, false, 2000);
        
        assert_eq!(stats.total_updates_analyzed, 2);
        assert_eq!(stats.sparse_updates_detected, 1);
        assert_eq!(stats.full_updates_detected, 1);
        assert_eq!(stats.average_analysis_time_ns(), 1500.0);
        assert_eq!(stats.change_detection_ratio(), 0.5); // 10 changed out of 20 total
        assert_eq!(stats.sparse_update_ratio(), 0.5);
    }

    #[test]
    fn test_incremental_optimization() {
        let mut analyzer = UpdateAnalyzer::with_default_config();
        let table_map = TableMapEvent::default();

        let before = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(Int(2)),
            Some(Int(3)),
            Some(Int(4)),
            Some(Int(5)),
        ]);
        
        let after = RowData::new_with_cells(vec![
            Some(Int(1)),
            Some(Int(20)), // Only this column changes
            Some(Int(3)),
            Some(Int(4)),
            Some(Int(5)),
        ]);

        let mut updates = vec![UpdateRowData::new(before, after)];
        let incremental_updates = analyzer.optimize_updates_to_incremental(&mut updates, &table_map).unwrap();
        
        assert_eq!(incremental_updates.len(), 1);
        let incremental = &incremental_updates[0];
        assert_eq!(incremental.changed_count(), 1);
        assert!(incremental.is_sparse_update(50.0));
    }
}
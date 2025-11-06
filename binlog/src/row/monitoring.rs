use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use common::err::decode_error::ReError;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::{RowData, UpdateRowData};

/// Comprehensive row parsing monitoring and statistics system
#[derive(Debug, Clone)]
pub struct RowParsingMonitor {
    /// Basic parsing statistics
    pub basic_stats: BasicParsingStats,
    /// Row data size and complexity analysis
    pub complexity_stats: RowComplexityStats,
    /// Error and exception statistics
    pub error_stats: ErrorStats,
    /// Performance metrics by table
    pub table_metrics: HashMap<String, TableMetrics>,
    /// Real-time monitoring data
    pub realtime_metrics: RealtimeMetrics,
    /// Configuration for monitoring
    pub config: MonitoringConfig,
}

/// Basic parsing statistics
#[derive(Debug, Default, Clone)]
pub struct BasicParsingStats {
    /// Total number of rows parsed
    pub total_rows_parsed: u64,
    /// Total bytes processed
    pub total_bytes_processed: u64,
    /// Total parsing time in nanoseconds
    pub total_parse_time_ns: u64,
    /// Number of INSERT operations
    pub insert_operations: u64,
    /// Number of UPDATE operations
    pub update_operations: u64,
    /// Number of DELETE operations
    pub delete_operations: u64,
    /// Memory allocations count
    pub memory_allocations: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Start time of monitoring
    pub start_time: Option<Instant>,
}

/// Row data size and complexity analysis
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RowComplexityStats {
    /// Average row size in bytes
    pub avg_row_size_bytes: f64,
    /// Minimum row size encountered
    pub min_row_size_bytes: u64,
    /// Maximum row size encountered
    pub max_row_size_bytes: u64,
    /// Average number of columns per row
    pub avg_columns_per_row: f64,
    /// Maximum number of columns in a single row
    pub max_columns_per_row: usize,
    /// Distribution of row sizes (size_range -> count)
    pub row_size_distribution: HashMap<String, u64>,
    /// Column type distribution (type_name -> count)
    pub column_type_distribution: HashMap<String, u64>,
    /// Null value statistics
    pub null_value_stats: NullValueStats,
    /// Large object (LOB) statistics
    pub lob_stats: LobStats,
    /// Update complexity statistics
    pub update_complexity: UpdateComplexityStats,
}

/// Null value statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NullValueStats {
    /// Total null values encountered
    pub total_null_values: u64,
    /// Null values per column index
    pub null_values_by_column: HashMap<usize, u64>,
    /// Percentage of null values
    pub null_percentage: f64,
}

/// Large object (LOB) statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LobStats {
    /// Number of BLOB/TEXT columns processed
    pub lob_columns_processed: u64,
    /// Total size of LOB data in bytes
    pub total_lob_size_bytes: u64,
    /// Average LOB size
    pub avg_lob_size_bytes: f64,
    /// Maximum LOB size encountered
    pub max_lob_size_bytes: u64,
    /// LOB size distribution
    pub lob_size_distribution: HashMap<String, u64>,
}

/// Update operation complexity statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UpdateComplexityStats {
    /// Average percentage of columns changed per update
    pub avg_change_percentage: f64,
    /// Number of sparse updates (< 25% columns changed)
    pub sparse_updates: u64,
    /// Number of full updates (> 75% columns changed)
    pub full_updates: u64,
    /// Distribution of change percentages
    pub change_percentage_distribution: HashMap<String, u64>,
    /// Field-level change statistics
    pub field_change_stats: HashMap<usize, u64>,
}

/// Error and exception statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ErrorStats {
    /// Total number of errors encountered
    pub total_errors: u64,
    /// Errors by type
    pub errors_by_type: HashMap<String, u64>,
    /// Errors by table
    pub errors_by_table: HashMap<String, u64>,
    /// Parse errors (malformed data)
    pub parse_errors: u64,
    /// IO errors
    pub io_errors: u64,
    /// Memory errors
    pub memory_errors: u64,
    /// Timeout errors
    pub timeout_errors: u64,
    /// Recoverable errors (continued parsing)
    pub recoverable_errors: u64,
    /// Fatal errors (stopped parsing)
    pub fatal_errors: u64,
    /// Error recovery statistics
    pub error_recovery_stats: ErrorRecoveryStats,
}

/// Error recovery statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ErrorRecoveryStats {
    /// Number of successful error recoveries
    pub successful_recoveries: u64,
    /// Number of failed recovery attempts
    pub failed_recoveries: u64,
    /// Average recovery time in nanoseconds
    pub avg_recovery_time_ns: u64,
    /// Rows skipped due to errors
    pub rows_skipped: u64,
}

/// Performance metrics per table
#[derive(Debug, Default, Clone)]
pub struct TableMetrics {
    /// Table name
    pub table_name: String,
    /// Database name
    pub database_name: String,
    /// Number of rows processed for this table
    pub rows_processed: u64,
    /// Total bytes processed for this table
    pub bytes_processed: u64,
    /// Total processing time for this table
    pub processing_time_ns: u64,
    /// Average row size for this table
    pub avg_row_size: f64,
    /// Operations by type
    pub operations: OperationStats,
    /// Error count for this table
    pub error_count: u64,
    /// Last processing time
    pub last_processed: Option<Instant>,
}

/// Operation statistics per table
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct OperationStats {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
}

/// Real-time monitoring metrics
#[derive(Debug, Default, Clone)]
pub struct RealtimeMetrics {
    /// Current parsing rate (rows per second)
    pub current_rows_per_second: f64,
    /// Current throughput (bytes per second)
    pub current_bytes_per_second: f64,
    /// Moving average of parse times (last N operations)
    pub moving_avg_parse_time_ns: f64,
    /// Peak memory usage during parsing
    pub peak_memory_usage_bytes: u64,
    /// Current memory usage
    pub current_memory_usage_bytes: u64,
    /// CPU usage percentage (if available)
    pub cpu_usage_percentage: f64,
    /// Queue depth for async operations
    pub queue_depth: usize,
    /// Last update timestamp
    pub last_update: Option<Instant>,
}

/// Configuration for monitoring behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable detailed complexity analysis
    pub enable_complexity_analysis: bool,
    /// Enable per-table metrics
    pub enable_table_metrics: bool,
    /// Enable real-time metrics
    pub enable_realtime_metrics: bool,
    /// Sample rate for detailed analysis (1.0 = all rows, 0.1 = 10% of rows)
    pub sample_rate: f64,
    /// Maximum number of tables to track individually
    pub max_tracked_tables: usize,
    /// Moving average window size for real-time metrics
    pub moving_average_window: usize,
    /// Memory usage tracking interval in milliseconds
    pub memory_tracking_interval_ms: u64,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enable_complexity_analysis: true,
            enable_table_metrics: true,
            enable_realtime_metrics: true,
            sample_rate: 1.0,
            max_tracked_tables: 1000,
            moving_average_window: 100,
            memory_tracking_interval_ms: 1000,
        }
    }
}

impl RowParsingMonitor {
    /// Create a new monitoring instance with default configuration
    pub fn new() -> Self {
        Self::with_config(MonitoringConfig::default())
    }

    /// Create a new monitoring instance with custom configuration
    pub fn with_config(config: MonitoringConfig) -> Self {
        let mut monitor = Self {
            basic_stats: BasicParsingStats::default(),
            complexity_stats: RowComplexityStats::default(),
            error_stats: ErrorStats::default(),
            table_metrics: HashMap::new(),
            realtime_metrics: RealtimeMetrics::default(),
            config,
        };
        monitor.basic_stats.start_time = Some(Instant::now());
        monitor
    }

    /// Record a row parsing operation
    pub fn record_row_parsed(
        &mut self,
        table_map: &TableMapEvent,
        row_data: &RowData,
        parse_time: Duration,
        bytes_processed: u64,
    ) {
        // Update basic statistics
        self.basic_stats.total_rows_parsed += 1;
        self.basic_stats.total_bytes_processed += bytes_processed;
        self.basic_stats.total_parse_time_ns += parse_time.as_nanos() as u64;

        // Update complexity analysis if enabled
        if self.config.enable_complexity_analysis && self.should_sample() {
            self.analyze_row_complexity(row_data, bytes_processed);
        }

        // Update table metrics if enabled
        if self.config.enable_table_metrics {
            self.update_table_metrics(table_map, bytes_processed, parse_time);
        }

        // Update real-time metrics if enabled
        if self.config.enable_realtime_metrics {
            self.update_realtime_metrics(parse_time, bytes_processed);
        }
    }

    /// Record an INSERT operation
    pub fn record_insert_operation(&mut self, table_map: &TableMapEvent, rows: &[RowData]) {
        self.basic_stats.insert_operations += 1;
        
        if self.config.enable_table_metrics {
            let table_key = self.get_table_key(table_map);
            let metrics = self.table_metrics.entry(table_key).or_insert_with(|| {
                TableMetrics {
                    table_name: table_map.get_table_name(),
                    database_name: table_map.get_database_name(),
                    ..Default::default()
                }
            });
            metrics.operations.inserts += rows.len() as u64;
            metrics.last_processed = Some(Instant::now());
        }
    }

    /// Record an UPDATE operation
    pub fn record_update_operation(&mut self, table_map: &TableMapEvent, updates: &[UpdateRowData]) {
        self.basic_stats.update_operations += 1;

        // Analyze update complexity if enabled
        if self.config.enable_complexity_analysis && self.should_sample() {
            for update in updates {
                self.analyze_update_complexity(update);
            }
        }
        
        if self.config.enable_table_metrics {
            let table_key = self.get_table_key(table_map);
            let metrics = self.table_metrics.entry(table_key).or_insert_with(|| {
                TableMetrics {
                    table_name: table_map.get_table_name(),
                    database_name: table_map.get_database_name(),
                    ..Default::default()
                }
            });
            metrics.operations.updates += updates.len() as u64;
            metrics.last_processed = Some(Instant::now());
        }
    }

    /// Record a DELETE operation
    pub fn record_delete_operation(&mut self, table_map: &TableMapEvent, rows: &[RowData]) {
        self.basic_stats.delete_operations += 1;
        
        if self.config.enable_table_metrics {
            let table_key = self.get_table_key(table_map);
            let metrics = self.table_metrics.entry(table_key).or_insert_with(|| {
                TableMetrics {
                    table_name: table_map.get_table_name(),
                    database_name: table_map.get_database_name(),
                    ..Default::default()
                }
            });
            metrics.operations.deletes += rows.len() as u64;
            metrics.last_processed = Some(Instant::now());
        }
    }

    /// Record an error during parsing
    pub fn record_error(&mut self, error: &ReError, table_map: Option<&TableMapEvent>, is_recoverable: bool) {
        self.error_stats.total_errors += 1;

        // Categorize error by type
        let error_type = self.categorize_error(error);
        *self.error_stats.errors_by_type.entry(error_type).or_insert(0) += 1;

        // Track error by table if available
        if let Some(table) = table_map {
            let table_key = self.get_table_key(table);
            *self.error_stats.errors_by_table.entry(table_key.clone()).or_insert(0) += 1;
            
            if self.config.enable_table_metrics {
                if let Some(metrics) = self.table_metrics.get_mut(&table_key) {
                    metrics.error_count += 1;
                }
            }
        }

        // Update error type counters
        match error {
            ReError::IoError(_) => self.error_stats.io_errors += 1,
            ReError::String(msg) if msg.contains("memory") => self.error_stats.memory_errors += 1,
            ReError::String(msg) if msg.contains("timeout") => self.error_stats.timeout_errors += 1,
            _ => self.error_stats.parse_errors += 1,
        }

        // Track recovery status
        if is_recoverable {
            self.error_stats.recoverable_errors += 1;
        } else {
            self.error_stats.fatal_errors += 1;
        }
    }

    /// Record successful error recovery
    pub fn record_error_recovery(&mut self, recovery_time: Duration, rows_skipped: u64) {
        self.error_stats.error_recovery_stats.successful_recoveries += 1;
        self.error_stats.error_recovery_stats.rows_skipped += rows_skipped;
        
        // Update average recovery time
        let current_avg = self.error_stats.error_recovery_stats.avg_recovery_time_ns;
        let new_time = recovery_time.as_nanos() as u64;
        let total_recoveries = self.error_stats.error_recovery_stats.successful_recoveries;
        
        self.error_stats.error_recovery_stats.avg_recovery_time_ns = 
            (current_avg * (total_recoveries - 1) + new_time) / total_recoveries;
    }

    /// Record failed error recovery
    pub fn record_error_recovery_failure(&mut self) {
        self.error_stats.error_recovery_stats.failed_recoveries += 1;
    }

    /// Record cache hit
    pub fn record_cache_hit(&mut self) {
        self.basic_stats.cache_hits += 1;
    }

    /// Record cache miss
    pub fn record_cache_miss(&mut self) {
        self.basic_stats.cache_misses += 1;
    }

    /// Record memory allocation
    pub fn record_memory_allocation(&mut self) {
        self.basic_stats.memory_allocations += 1;
    }

    /// Get comprehensive statistics report
    pub fn get_statistics_report(&self) -> StatisticsReport {
        StatisticsReport {
            basic_stats: self.basic_stats.clone(),
            complexity_stats: self.complexity_stats.clone(),
            error_stats: self.error_stats.clone(),
            realtime_metrics: self.realtime_metrics.clone(),
            computed_metrics: self.compute_derived_metrics(),
            top_tables: self.get_top_tables_by_activity(10),
            monitoring_duration: self.get_monitoring_duration(),
        }
    }

    /// Reset all statistics
    pub fn reset_statistics(&mut self) {
        self.basic_stats = BasicParsingStats::default();
        self.complexity_stats = RowComplexityStats::default();
        self.error_stats = ErrorStats::default();
        self.table_metrics.clear();
        self.realtime_metrics = RealtimeMetrics::default();
        self.basic_stats.start_time = Some(Instant::now());
    }

    /// Get monitoring configuration
    pub fn get_config(&self) -> &MonitoringConfig {
        &self.config
    }

    /// Update monitoring configuration
    pub fn update_config(&mut self, config: MonitoringConfig) {
        self.config = config;
    }

    // Private helper methods

    fn should_sample(&self) -> bool {
        if self.config.sample_rate >= 1.0 {
            true
        } else {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            
            let mut hasher = DefaultHasher::new();
            self.basic_stats.total_rows_parsed.hash(&mut hasher);
            let hash = hasher.finish();
            
            (hash as f64 / u64::MAX as f64) < self.config.sample_rate
        }
    }

    fn analyze_row_complexity(&mut self, row_data: &RowData, bytes_processed: u64) {
        let column_count = row_data.cells.len();
        
        // Update size statistics
        self.update_size_statistics(bytes_processed);
        
        // Update column statistics
        self.update_column_statistics(column_count, &row_data.cells);
        
        // Update null value statistics
        self.update_null_statistics(&row_data.cells);
        
        // Update LOB statistics
        self.update_lob_statistics(&row_data.cells);
    }

    fn analyze_update_complexity(&mut self, update_data: &UpdateRowData) {
        if let Some(diff) = update_data.get_difference_readonly() {
            let change_percentage = diff.change_percentage();
            
            // Update average change percentage
            let total_updates = self.complexity_stats.update_complexity.sparse_updates + 
                              self.complexity_stats.update_complexity.full_updates + 1;
            let current_avg = self.complexity_stats.update_complexity.avg_change_percentage;
            self.complexity_stats.update_complexity.avg_change_percentage = 
                (current_avg * (total_updates - 1) as f64 + change_percentage) / total_updates as f64;
            
            // Categorize update type
            if change_percentage < 25.0 {
                self.complexity_stats.update_complexity.sparse_updates += 1;
            } else if change_percentage > 75.0 {
                self.complexity_stats.update_complexity.full_updates += 1;
            }
            
            // Update change percentage distribution
            let range = self.get_percentage_range(change_percentage);
            *self.complexity_stats.update_complexity.change_percentage_distribution
                .entry(range).or_insert(0) += 1;
            
            // Update field-level change statistics
            for change in &diff.changed_fields {
                *self.complexity_stats.update_complexity.field_change_stats
                    .entry(change.column_index).or_insert(0) += 1;
            }
        }
    }

    fn update_size_statistics(&mut self, bytes_processed: u64) {
        // Update min/max
        if self.complexity_stats.min_row_size_bytes == 0 || bytes_processed < self.complexity_stats.min_row_size_bytes {
            self.complexity_stats.min_row_size_bytes = bytes_processed;
        }
        if bytes_processed > self.complexity_stats.max_row_size_bytes {
            self.complexity_stats.max_row_size_bytes = bytes_processed;
        }
        
        // Update average
        let total_rows = self.basic_stats.total_rows_parsed;
        let current_avg = self.complexity_stats.avg_row_size_bytes;
        self.complexity_stats.avg_row_size_bytes = 
            (current_avg * (total_rows - 1) as f64 + bytes_processed as f64) / total_rows as f64;
        
        // Update size distribution
        let size_range = self.get_size_range(bytes_processed);
        *self.complexity_stats.row_size_distribution.entry(size_range).or_insert(0) += 1;
    }

    fn update_column_statistics(&mut self, column_count: usize, cells: &[Option<common::binlog::column::column_value::SrcColumnValue>]) {
        // Update max columns
        if column_count > self.complexity_stats.max_columns_per_row {
            self.complexity_stats.max_columns_per_row = column_count;
        }
        
        // Update average columns per row
        let total_rows = self.basic_stats.total_rows_parsed;
        let current_avg = self.complexity_stats.avg_columns_per_row;
        self.complexity_stats.avg_columns_per_row = 
            (current_avg * (total_rows - 1) as f64 + column_count as f64) / total_rows as f64;
        
        // Update column type distribution
        for cell in cells {
            if let Some(value) = cell {
                let type_name = self.get_column_type_name(value);
                *self.complexity_stats.column_type_distribution.entry(type_name).or_insert(0) += 1;
            }
        }
    }

    fn update_null_statistics(&mut self, cells: &[Option<common::binlog::column::column_value::SrcColumnValue>]) {
        for (index, cell) in cells.iter().enumerate() {
            if cell.is_none() {
                self.complexity_stats.null_value_stats.total_null_values += 1;
                *self.complexity_stats.null_value_stats.null_values_by_column
                    .entry(index).or_insert(0) += 1;
            }
        }
        
        // Update null percentage
        let total_cells = self.basic_stats.total_rows_parsed * self.complexity_stats.avg_columns_per_row as u64;
        if total_cells > 0 {
            self.complexity_stats.null_value_stats.null_percentage = 
                (self.complexity_stats.null_value_stats.total_null_values as f64 / total_cells as f64) * 100.0;
        }
    }

    fn update_lob_statistics(&mut self, cells: &[Option<common::binlog::column::column_value::SrcColumnValue>]) {
        use common::binlog::column::column_value::SrcColumnValue;
        
        for cell in cells {
            if let Some(value) = cell {
                match value {
                    SrcColumnValue::Blob(data) => {
                        self.complexity_stats.lob_stats.lob_columns_processed += 1;
                        let size = data.len() as u64;
                        self.complexity_stats.lob_stats.total_lob_size_bytes += size;
                        
                        if size > self.complexity_stats.lob_stats.max_lob_size_bytes {
                            self.complexity_stats.lob_stats.max_lob_size_bytes = size;
                        }
                        
                        // Update average LOB size
                        let lob_count = self.complexity_stats.lob_stats.lob_columns_processed;
                        self.complexity_stats.lob_stats.avg_lob_size_bytes = 
                            self.complexity_stats.lob_stats.total_lob_size_bytes as f64 / lob_count as f64;
                        
                        // Update LOB size distribution
                        let size_range = self.get_lob_size_range(size);
                        *self.complexity_stats.lob_stats.lob_size_distribution.entry(size_range).or_insert(0) += 1;
                    }
                    _ => {}
                }
            }
        }
    }

    fn update_table_metrics(&mut self, table_map: &TableMapEvent, bytes_processed: u64, parse_time: Duration) {
        let table_key = self.get_table_key(table_map);
        
        // Limit the number of tracked tables
        if self.table_metrics.len() >= self.config.max_tracked_tables && !self.table_metrics.contains_key(&table_key) {
            return;
        }
        
        let metrics = self.table_metrics.entry(table_key).or_insert_with(|| {
            TableMetrics {
                table_name: table_map.get_table_name(),
                database_name: table_map.get_database_name(),
                ..Default::default()
            }
        });
        
        metrics.rows_processed += 1;
        metrics.bytes_processed += bytes_processed;
        metrics.processing_time_ns += parse_time.as_nanos() as u64;
        
        // Update average row size for this table
        metrics.avg_row_size = metrics.bytes_processed as f64 / metrics.rows_processed as f64;
        metrics.last_processed = Some(Instant::now());
    }

    fn update_realtime_metrics(&mut self, parse_time: Duration, bytes_processed: u64) {
        let now = Instant::now();
        
        // Update moving average parse time
        let window_size = self.config.moving_average_window as f64;
        let current_avg = self.realtime_metrics.moving_avg_parse_time_ns;
        let new_time = parse_time.as_nanos() as f64;
        
        self.realtime_metrics.moving_avg_parse_time_ns = 
            (current_avg * (window_size - 1.0) + new_time) / window_size;
        
        // Calculate current rates if we have a previous update
        if let Some(last_update) = self.realtime_metrics.last_update {
            let time_diff = now.duration_since(last_update).as_secs_f64();
            if time_diff > 0.0 {
                self.realtime_metrics.current_rows_per_second = 1.0 / time_diff;
                self.realtime_metrics.current_bytes_per_second = bytes_processed as f64 / time_diff;
            }
        }
        
        self.realtime_metrics.last_update = Some(now);
    }

    fn get_table_key(&self, table_map: &TableMapEvent) -> String {
        format!("{}.{}", table_map.get_database_name(), table_map.get_table_name())
    }

    fn categorize_error(&self, error: &ReError) -> String {
        match error {
            ReError::IoError(_) => "IO Error".to_string(),
            ReError::String(msg) => {
                if msg.contains("parse") || msg.contains("decode") {
                    "Parse Error".to_string()
                } else if msg.contains("memory") {
                    "Memory Error".to_string()
                } else if msg.contains("timeout") {
                    "Timeout Error".to_string()
                } else {
                    "General Error".to_string()
                }
            }
            _ => "Unknown Error".to_string(),
        }
    }

    fn get_size_range(&self, size: u64) -> String {
        match size {
            0..=100 => "0-100B".to_string(),
            101..=1000 => "101B-1KB".to_string(),
            1001..=10000 => "1-10KB".to_string(),
            10001..=100000 => "10-100KB".to_string(),
            100001..=1000000 => "100KB-1MB".to_string(),
            _ => ">1MB".to_string(),
        }
    }

    fn get_lob_size_range(&self, size: u64) -> String {
        match size {
            0..=1024 => "0-1KB".to_string(),
            1025..=10240 => "1-10KB".to_string(),
            10241..=102400 => "10-100KB".to_string(),
            102401..=1048576 => "100KB-1MB".to_string(),
            1048577..=10485760 => "1-10MB".to_string(),
            _ => ">10MB".to_string(),
        }
    }

    fn get_percentage_range(&self, percentage: f64) -> String {
        match percentage as u32 {
            0..=10 => "0-10%".to_string(),
            11..=25 => "11-25%".to_string(),
            26..=50 => "26-50%".to_string(),
            51..=75 => "51-75%".to_string(),
            76..=90 => "76-90%".to_string(),
            _ => "91-100%".to_string(),
        }
    }

    fn get_column_type_name(&self, value: &common::binlog::column::column_value::SrcColumnValue) -> String {
        use common::binlog::column::column_value::SrcColumnValue;
        
        match value {
            SrcColumnValue::TinyInt(_) => "TinyInt".to_string(),
            SrcColumnValue::SmallInt(_) => "SmallInt".to_string(),
            SrcColumnValue::MediumInt(_) => "MediumInt".to_string(),
            SrcColumnValue::Int(_) => "Int".to_string(),
            SrcColumnValue::BigInt(_) => "BigInt".to_string(),
            SrcColumnValue::Float(_) => "Float".to_string(),
            SrcColumnValue::Double(_) => "Double".to_string(),
            SrcColumnValue::Decimal(_) => "Decimal".to_string(),
            SrcColumnValue::String(_) => "String".to_string(),
            SrcColumnValue::Blob(_) => "Blob".to_string(),
            SrcColumnValue::Date(_) => "Date".to_string(),
            SrcColumnValue::Time(_) => "Time".to_string(),
            SrcColumnValue::DateTime(_) => "DateTime".to_string(),
            SrcColumnValue::Timestamp(_) => "Timestamp".to_string(),
            SrcColumnValue::Year(_) => "Year".to_string(),
            SrcColumnValue::Bit(_) => "Bit".to_string(),
            SrcColumnValue::Enum(_) => "Enum".to_string(),
            SrcColumnValue::Set(_) => "Set".to_string(),
        }
    }

    fn compute_derived_metrics(&self) -> ComputedMetrics {
        let total_time = self.get_monitoring_duration();
        
        ComputedMetrics {
            overall_throughput_rows_per_second: if total_time.as_secs() > 0 {
                self.basic_stats.total_rows_parsed as f64 / total_time.as_secs_f64()
            } else {
                0.0
            },
            overall_throughput_bytes_per_second: if total_time.as_secs() > 0 {
                self.basic_stats.total_bytes_processed as f64 / total_time.as_secs_f64()
            } else {
                0.0
            },
            average_parse_time_ns: if self.basic_stats.total_rows_parsed > 0 {
                self.basic_stats.total_parse_time_ns as f64 / self.basic_stats.total_rows_parsed as f64
            } else {
                0.0
            },
            cache_hit_ratio: {
                let total_accesses = self.basic_stats.cache_hits + self.basic_stats.cache_misses;
                if total_accesses > 0 {
                    self.basic_stats.cache_hits as f64 / total_accesses as f64
                } else {
                    0.0
                }
            },
            error_rate: if self.basic_stats.total_rows_parsed > 0 {
                self.error_stats.total_errors as f64 / self.basic_stats.total_rows_parsed as f64
            } else {
                0.0
            },
            recovery_success_rate: {
                let total_recovery_attempts = self.error_stats.error_recovery_stats.successful_recoveries + 
                                            self.error_stats.error_recovery_stats.failed_recoveries;
                if total_recovery_attempts > 0 {
                    self.error_stats.error_recovery_stats.successful_recoveries as f64 / total_recovery_attempts as f64
                } else {
                    0.0
                }
            },
        }
    }

    fn get_top_tables_by_activity(&self, limit: usize) -> Vec<TableMetrics> {
        let mut tables: Vec<_> = self.table_metrics.values().cloned().collect();
        tables.sort_by(|a, b| b.rows_processed.cmp(&a.rows_processed));
        tables.truncate(limit);
        tables
    }

    fn get_monitoring_duration(&self) -> Duration {
        if let Some(start_time) = self.basic_stats.start_time {
            Instant::now().duration_since(start_time)
        } else {
            Duration::from_secs(0)
        }
    }
}

impl Default for RowParsingMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Computed metrics derived from basic statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedMetrics {
    pub overall_throughput_rows_per_second: f64,
    pub overall_throughput_bytes_per_second: f64,
    pub average_parse_time_ns: f64,
    pub cache_hit_ratio: f64,
    pub error_rate: f64,
    pub recovery_success_rate: f64,
}

/// Comprehensive statistics report
#[derive(Debug, Clone)]
pub struct StatisticsReport {
    pub basic_stats: BasicParsingStats,
    pub complexity_stats: RowComplexityStats,
    pub error_stats: ErrorStats,
    pub realtime_metrics: RealtimeMetrics,
    pub computed_metrics: ComputedMetrics,
    pub top_tables: Vec<TableMetrics>,
    pub monitoring_duration: Duration,
}

impl StatisticsReport {
    /// Generate a human-readable summary of the statistics
    pub fn generate_summary(&self) -> String {
        format!(
            "Row Parsing Statistics Summary\n\
            ==============================\n\
            Monitoring Duration: {:.2}s\n\
            Total Rows Parsed: {}\n\
            Total Bytes Processed: {} ({:.2} MB)\n\
            Average Parse Time: {:.2}μs\n\
            Throughput: {:.2} rows/sec, {:.2} MB/sec\n\
            Cache Hit Ratio: {:.2}%\n\
            Error Rate: {:.4}%\n\
            Operations: {} inserts, {} updates, {} deletes\n\
            Average Row Size: {:.2} bytes\n\
            Max Row Size: {} bytes\n\
            Null Value Percentage: {:.2}%\n\
            LOB Columns Processed: {}\n\
            Total Errors: {} (Recoverable: {}, Fatal: {})\n\
            Top Table: {} ({} rows)",
            self.monitoring_duration.as_secs_f64(),
            self.basic_stats.total_rows_parsed,
            self.basic_stats.total_bytes_processed,
            self.basic_stats.total_bytes_processed as f64 / 1_048_576.0,
            self.computed_metrics.average_parse_time_ns / 1000.0,
            self.computed_metrics.overall_throughput_rows_per_second,
            self.computed_metrics.overall_throughput_bytes_per_second / 1_048_576.0,
            self.computed_metrics.cache_hit_ratio * 100.0,
            self.computed_metrics.error_rate * 100.0,
            self.basic_stats.insert_operations,
            self.basic_stats.update_operations,
            self.basic_stats.delete_operations,
            self.complexity_stats.avg_row_size_bytes,
            self.complexity_stats.max_row_size_bytes,
            self.complexity_stats.null_value_stats.null_percentage,
            self.complexity_stats.lob_stats.lob_columns_processed,
            self.error_stats.total_errors,
            self.error_stats.recoverable_errors,
            self.error_stats.fatal_errors,
            self.top_tables.first().map(|t| t.table_name.as_str()).unwrap_or("N/A"),
            self.top_tables.first().map(|t| t.rows_processed).unwrap_or(0)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use common::binlog::column::column_value::SrcColumnValue;
    use crate::events::protocol::table_map_event::TableMapEvent;

    #[test]
    fn test_row_parsing_monitor_creation() {
        let monitor = RowParsingMonitor::new();
        assert_eq!(monitor.basic_stats.total_rows_parsed, 0);
        assert!(monitor.basic_stats.start_time.is_some());
        assert!(monitor.config.enable_complexity_analysis);
    }

    #[test]
    fn test_record_row_parsed() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let row_data = RowData::new_with_cells(vec![
            Some(SrcColumnValue::Int(42)),
            Some(SrcColumnValue::String("test".to_string())),
            None,
        ]);
        
        monitor.record_row_parsed(&table_map, &row_data, Duration::from_millis(1), 100);
        
        assert_eq!(monitor.basic_stats.total_rows_parsed, 1);
        assert_eq!(monitor.basic_stats.total_bytes_processed, 100);
        assert!(monitor.basic_stats.total_parse_time_ns > 0);
    }

    #[test]
    fn test_record_operations() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let rows = vec![RowData::new()];
        
        monitor.record_insert_operation(&table_map, &rows);
        assert_eq!(monitor.basic_stats.insert_operations, 1);
        
        monitor.record_delete_operation(&table_map, &rows);
        assert_eq!(monitor.basic_stats.delete_operations, 1);
    }

    #[test]
    fn test_error_recording() {
        let mut monitor = RowParsingMonitor::new();
        let error = ReError::String("Test error".to_string());
        
        monitor.record_error(&error, None, true);
        assert_eq!(monitor.error_stats.total_errors, 1);
        assert_eq!(monitor.error_stats.recoverable_errors, 1);
        assert_eq!(monitor.error_stats.fatal_errors, 0);
    }

    #[test]
    fn test_cache_statistics() {
        let mut monitor = RowParsingMonitor::new();
        
        monitor.record_cache_hit();
        monitor.record_cache_hit();
        monitor.record_cache_miss();
        
        assert_eq!(monitor.basic_stats.cache_hits, 2);
        assert_eq!(monitor.basic_stats.cache_misses, 1);
        
        let report = monitor.get_statistics_report();
        assert_eq!(report.computed_metrics.cache_hit_ratio, 2.0 / 3.0);
    }

    #[test]
    fn test_complexity_analysis() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let row_data = RowData::new_with_cells(vec![
            Some(SrcColumnValue::Int(42)),
            Some(SrcColumnValue::Blob(vec![1, 2, 3, 4, 5])),
            None,
        ]);
        
        monitor.record_row_parsed(&table_map, &row_data, Duration::from_millis(1), 100);
        
        assert_eq!(monitor.complexity_stats.max_columns_per_row, 3);
        assert_eq!(monitor.complexity_stats.null_value_stats.total_null_values, 1);
        assert_eq!(monitor.complexity_stats.lob_stats.lob_columns_processed, 1);
    }

    #[test]
    fn test_statistics_report() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let row_data = RowData::new();
        
        monitor.record_row_parsed(&table_map, &row_data, Duration::from_millis(1), 50);
        
        let report = monitor.get_statistics_report();
        assert_eq!(report.basic_stats.total_rows_parsed, 1);
        assert!(report.computed_metrics.average_parse_time_ns > 0.0);
        
        let summary = report.generate_summary();
        assert!(summary.contains("Total Rows Parsed: 1"));
    }

    #[test]
    fn test_monitoring_config() {
        let config = MonitoringConfig {
            enable_complexity_analysis: false,
            sample_rate: 0.5,
            ..Default::default()
        };
        
        let monitor = RowParsingMonitor::with_config(config);
        assert!(!monitor.config.enable_complexity_analysis);
        assert_eq!(monitor.config.sample_rate, 0.5);
    }

    #[test]
    fn test_reset_statistics() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let row_data = RowData::new();
        
        monitor.record_row_parsed(&table_map, &row_data, Duration::from_millis(1), 50);
        assert_eq!(monitor.basic_stats.total_rows_parsed, 1);
        
        monitor.reset_statistics();
        assert_eq!(monitor.basic_stats.total_rows_parsed, 0);
        assert!(monitor.basic_stats.start_time.is_some());
    }
}
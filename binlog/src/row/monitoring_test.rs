use std::time::Duration;
use common::binlog::column::column_value::SrcColumnValue;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::RowData;
use crate::row::monitoring::{RowParsingMonitor, MonitoringConfig};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitoring_integration() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        
        // Test row parsing monitoring
        let row_data = RowData::new_with_cells(vec![
            Some(SrcColumnValue::Int(42)),
            Some(SrcColumnValue::String("test".to_string())),
            None,
        ]);
        
        monitor.record_row_parsed(&table_map, &row_data, Duration::from_millis(1), 100);
        
        let report = monitor.get_statistics_report();
        assert_eq!(report.basic_stats.total_rows_parsed, 1);
        assert_eq!(report.basic_stats.total_bytes_processed, 100);
        assert!(report.basic_stats.total_parse_time_ns > 0);
        
        // Test complexity analysis
        assert_eq!(report.complexity_stats.max_columns_per_row, 3);
        assert_eq!(report.complexity_stats.null_value_stats.total_null_values, 1);
        
        // Test summary generation
        let summary = report.generate_summary();
        assert!(summary.contains("Total Rows Parsed: 1"));
        assert!(summary.contains("Total Bytes Processed: 100"));
    }

    #[test]
    fn test_monitoring_with_custom_config() {
        let config = MonitoringConfig {
            enable_complexity_analysis: false,
            enable_table_metrics: false,
            sample_rate: 0.5,
            ..Default::default()
        };
        
        let monitor = RowParsingMonitor::with_config(config);
        assert!(!monitor.get_config().enable_complexity_analysis);
        assert!(!monitor.get_config().enable_table_metrics);
        assert_eq!(monitor.get_config().sample_rate, 0.5);
    }

    #[test]
    fn test_error_monitoring() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let error = common::err::decode_error::ReError::String("Test error".to_string());
        
        monitor.record_error(&error, Some(&table_map), true);
        
        let report = monitor.get_statistics_report();
        assert_eq!(report.error_stats.total_errors, 1);
        assert_eq!(report.error_stats.recoverable_errors, 1);
        assert_eq!(report.error_stats.fatal_errors, 0);
    }

    #[test]
    fn test_operation_monitoring() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let rows = vec![RowData::new()];
        
        monitor.record_insert_operation(&table_map, &rows);
        monitor.record_delete_operation(&table_map, &rows);
        
        let report = monitor.get_statistics_report();
        assert_eq!(report.basic_stats.insert_operations, 1);
        assert_eq!(report.basic_stats.delete_operations, 1);
    }

    #[test]
    fn test_cache_statistics() {
        let mut monitor = RowParsingMonitor::new();
        
        monitor.record_cache_hit();
        monitor.record_cache_hit();
        monitor.record_cache_miss();
        
        let report = monitor.get_statistics_report();
        assert_eq!(report.basic_stats.cache_hits, 2);
        assert_eq!(report.basic_stats.cache_misses, 1);
        assert_eq!(report.computed_metrics.cache_hit_ratio, 2.0 / 3.0);
    }

    #[test]
    fn test_reset_statistics() {
        let mut monitor = RowParsingMonitor::new();
        let table_map = TableMapEvent::default();
        let row_data = RowData::new();
        
        monitor.record_row_parsed(&table_map, &row_data, Duration::from_millis(1), 50);
        assert_eq!(monitor.get_statistics_report().basic_stats.total_rows_parsed, 1);
        
        monitor.reset_statistics();
        assert_eq!(monitor.get_statistics_report().basic_stats.total_rows_parsed, 0);
    }
}
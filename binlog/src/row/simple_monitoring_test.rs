use std::time::Duration;
use common::binlog::column::column_value::SrcColumnValue;
use common::err::decode_error::ReError;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::{RowData, UpdateRowData};
use crate::row::parser::RowParser;
use crate::row::monitoring::MonitoringConfig;

#[cfg(test)]
mod simple_tests {
    use super::*;

    /// 创建测试用的表映射事件
    fn create_test_table_map(table_name: &str) -> TableMapEvent {
        let mut table_map = TableMapEvent::default();
        table_map.set_table_name(table_name.to_string());
        table_map
    }

    /// 创建测试用的行数据
    fn create_test_row_data(id: u32, name: &str, age: Option<u32>) -> RowData {
        RowData::new_with_cells(vec![
            Some(SrcColumnValue::Int(id)),
            Some(SrcColumnValue::String(name.to_string())),
            age.map(SrcColumnValue::Int),
        ])
    }

    #[test]
    fn test_basic_monitoring() {
        println!("\n=== 基础监控测试 ===");
        
        // 创建解析器
        let mut parser = RowParser::with_default_cache();
        let table_map = create_test_table_map("test_table");
        
        println!("✅ 创建解析器成功");
        
        // 模拟一些行解析
        let test_rows = vec![
            create_test_row_data(1, "Alice", Some(25)),
            create_test_row_data(2, "Bob", Some(30)),
            create_test_row_data(3, "Charlie", None),
        ];
        
        for (i, row) in test_rows.iter().enumerate() {
            parser.get_monitor_mut().record_row_parsed(
                &table_map, 
                row, 
                Duration::from_micros(10 + i as u64), 
                50 + i as u64 * 5
            );
        }
        
        // 记录操作
        parser.get_monitor_mut().record_insert_operation(&table_map, &test_rows);
        
        println!("✅ 记录了 {} 行数据", test_rows.len());
        
        // 获取统计报告
        let report = parser.get_statistics_report();
        
        println!("\n📊 统计报告:");
        println!("   总解析行数: {}", report.basic_stats.total_rows_parsed);
        println!("   总处理字节: {}", report.basic_stats.total_bytes_processed);
        println!("   INSERT操作: {}", report.basic_stats.insert_operations);
        println!("   平均解析时间: {:.2}μs", 
            report.computed_metrics.average_parse_time_ns / 1000.0);
        
        // 生成摘要
        let summary = parser.generate_monitoring_summary();
        println!("\n📋 摘要:");
        println!("{}", summary);
        
        println!("✅ 基础监控测试完成");
    }

    #[test]
    fn test_monitoring_config() {
        println!("\n=== 监控配置测试 ===");
        
        // 测试自定义配置
        let config = MonitoringConfig {
            enable_complexity_analysis: true,
            enable_table_metrics: true,
            enable_realtime_metrics: true,
            sample_rate: 0.5, // 50%采样率
            max_tracked_tables: 100,
            moving_average_window: 20,
            memory_tracking_interval_ms: 500,
        };
        
        let mut parser = RowParser::new_with_monitoring(500, config);
        println!("✅ 创建自定义监控解析器成功");
        
        // 验证配置
        let current_config = parser.get_monitor().get_config();
        println!("   采样率: {:.1}%", current_config.sample_rate * 100.0);
        println!("   复杂度分析: {}", current_config.enable_complexity_analysis);
        println!("   表级指标: {}", current_config.enable_table_metrics);
        
        // 更新配置
        let new_config = MonitoringConfig {
            sample_rate: 1.0, // 改为100%
            ..current_config.clone()
        };
        parser.update_monitoring_config(new_config);
        
        let updated_config = parser.get_monitor().get_config();
        println!("   更新后采样率: {:.1}%", updated_config.sample_rate * 100.0);
        
        println!("✅ 监控配置测试完成");
    }

    #[test]
    fn test_error_monitoring() {
        println!("\n=== 错误监控测试 ===");
        
        let mut parser = RowParser::with_default_cache();
        let table_map = create_test_table_map("error_test");
        
        // 模拟各种错误
        let errors = vec![
            ReError::String("Parse error".to_string()),
            ReError::String("Memory error".to_string()),
        ];
        
        for (i, error) in errors.iter().enumerate() {
            let is_recoverable = i == 0; // 第一个错误可恢复
            parser.get_monitor_mut().record_error(error, Some(&table_map), is_recoverable);
            
            if is_recoverable {
                parser.get_monitor_mut().record_error_recovery(Duration::from_millis(5), 1);
                println!("   记录可恢复错误并恢复成功");
            } else {
                parser.get_monitor_mut().record_error_recovery_failure();
                println!("   记录致命错误");
            }
        }
        
        let report = parser.get_statistics_report();
        println!("\n📊 错误统计:");
        println!("   总错误数: {}", report.error_stats.total_errors);
        println!("   可恢复错误: {}", report.error_stats.recoverable_errors);
        println!("   致命错误: {}", report.error_stats.fatal_errors);
        println!("   错误率: {:.4}%", report.computed_metrics.error_rate * 100.0);
        
        println!("✅ 错误监控测试完成");
    }

    #[test]
    fn test_cache_monitoring() {
        println!("\n=== 缓存监控测试 ===");
        
        let mut parser = RowParser::with_default_cache();
        
        // 模拟缓存操作
        for _ in 0..8 {
            parser.get_monitor_mut().record_cache_hit();
        }
        for _ in 0..2 {
            parser.get_monitor_mut().record_cache_miss();
        }
        
        let report = parser.get_statistics_report();
        println!("\n📊 缓存统计:");
        println!("   缓存命中: {}", report.basic_stats.cache_hits);
        println!("   缓存未命中: {}", report.basic_stats.cache_misses);
        println!("   命中率: {:.2}%", report.computed_metrics.cache_hit_ratio * 100.0);
        
        println!("✅ 缓存监控测试完成");
    }

    #[test]
    fn test_statistics_reset() {
        println!("\n=== 统计重置测试 ===");
        
        let mut parser = RowParser::with_default_cache();
        let table_map = create_test_table_map("reset_test");
        
        // 添加一些数据
        let row = create_test_row_data(1, "test", Some(100));
        parser.get_monitor_mut().record_row_parsed(&table_map, &row, Duration::from_micros(10), 50);
        
        let before_reset = parser.get_statistics_report();
        println!("   重置前行数: {}", before_reset.basic_stats.total_rows_parsed);
        
        // 重置统计
        parser.reset_monitoring_statistics();
        
        let after_reset = parser.get_statistics_report();
        println!("   重置后行数: {}", after_reset.basic_stats.total_rows_parsed);
        
        assert_eq!(after_reset.basic_stats.total_rows_parsed, 0);
        println!("✅ 统计重置测试完成");
    }

    #[test]
    fn test_comprehensive_output() {
        println!("\n=== 综合输出测试 ===");
        
        let config = MonitoringConfig {
            enable_complexity_analysis: true,
            enable_table_metrics: true,
            enable_realtime_metrics: true,
            sample_rate: 1.0,
            max_tracked_tables: 50,
            moving_average_window: 10,
            memory_tracking_interval_ms: 100,
        };
        
        let mut parser = RowParser::new_with_monitoring(500, config);
        let table_map = create_test_table_map("comprehensive_test");
        
        // 模拟复杂的数据处理
        let rows = vec![
            create_test_row_data(1, "Alice", Some(25)),
            create_test_row_data(2, "Bob", Some(30)),
            create_test_row_data(3, "Charlie", None),
            create_test_row_data(4, "Diana", Some(28)),
            create_test_row_data(5, "Eve", Some(35)),
        ];
        
        for (i, row) in rows.iter().enumerate() {
            parser.get_monitor_mut().record_row_parsed(
                &table_map, 
                row, 
                Duration::from_micros(8 + i as u64 * 2), 
                60 + i as u64 * 5
            );
        }
        
        parser.get_monitor_mut().record_insert_operation(&table_map, &rows);
        
        // 模拟UPDATE操作
        let update_before = create_test_row_data(1, "Alice", Some(25));
        let update_after = create_test_row_data(1, "Alice Smith", Some(26));
        let update_row = UpdateRowData::new_with_difference_detection(update_before, update_after);
        
        parser.get_monitor_mut().record_update_operation(&table_map, &vec![update_row]);
        
        // 模拟DELETE操作
        let delete_row = create_test_row_data(5, "Eve", Some(35));
        parser.get_monitor_mut().record_delete_operation(&table_map, &vec![delete_row]);
        
        // 模拟缓存和错误
        for _ in 0..15 {
            parser.get_monitor_mut().record_cache_hit();
        }
        for _ in 0..3 {
            parser.get_monitor_mut().record_cache_miss();
        }
        
        let error = ReError::String("Test error".to_string());
        parser.get_monitor_mut().record_error(&error, Some(&table_map), true);
        parser.get_monitor_mut().record_error_recovery(Duration::from_millis(2), 1);
        
        // 输出完整报告
        print_complete_report(&parser);
        
        println!("✅ 综合输出测试完成");
    }
    
    fn print_complete_report(parser: &RowParser) {
        println!("\n{}", "=".repeat(60));
        println!("                    完整监控报告");
        println!("{}", "=".repeat(60));
        
        let report = parser.get_statistics_report();
        
        // 基础统计
        println!("\n📈 基础统计:");
        println!("   总解析行数: {}", report.basic_stats.total_rows_parsed);
        println!("   总处理字节: {} bytes", report.basic_stats.total_bytes_processed);
        println!("   总解析时间: {:.2} ms", 
            report.basic_stats.total_parse_time_ns as f64 / 1_000_000.0);
        println!("   内存分配次数: {}", report.basic_stats.memory_allocations);
        
        // 操作统计
        println!("\n🔄 操作统计:");
        println!("   INSERT操作: {}", report.basic_stats.insert_operations);
        println!("   UPDATE操作: {}", report.basic_stats.update_operations);
        println!("   DELETE操作: {}", report.basic_stats.delete_operations);
        
        // 性能指标
        println!("\n⚡ 性能指标:");
        println!("   平均解析时间: {:.2}μs", 
            report.computed_metrics.average_parse_time_ns / 1000.0);
        println!("   整体吞吐量: {:.2} rows/sec", 
            report.computed_metrics.overall_throughput_rows_per_second);
        println!("   字节吞吐量: {:.2} KB/sec", 
            report.computed_metrics.overall_throughput_bytes_per_second / 1024.0);
        
        // 缓存统计
        println!("\n💾 缓存统计:");
        println!("   缓存命中: {}", report.basic_stats.cache_hits);
        println!("   缓存未命中: {}", report.basic_stats.cache_misses);
        println!("   命中率: {:.2}%", report.computed_metrics.cache_hit_ratio * 100.0);
        
        // 复杂度分析
        if report.complexity_stats.max_columns_per_row > 0 {
            println!("\n🔍 复杂度分析:");
            println!("   平均行大小: {:.2} bytes", report.complexity_stats.avg_row_size_bytes);
            println!("   最小行大小: {} bytes", report.complexity_stats.min_row_size_bytes);
            println!("   最大行大小: {} bytes", report.complexity_stats.max_row_size_bytes);
            println!("   平均列数: {:.1}", report.complexity_stats.avg_columns_per_row);
            println!("   最大列数: {}", report.complexity_stats.max_columns_per_row);
            
            // 空值统计
            println!("   空值统计:");
            println!("     总空值数: {}", report.complexity_stats.null_value_stats.total_null_values);
            println!("     空值百分比: {:.2}%", report.complexity_stats.null_value_stats.null_percentage);
        }
        
        // 错误统计
        if report.error_stats.total_errors > 0 {
            println!("\n❌ 错误统计:");
            println!("   总错误数: {}", report.error_stats.total_errors);
            println!("   错误率: {:.4}%", report.computed_metrics.error_rate * 100.0);
            println!("   可恢复错误: {}", report.error_stats.recoverable_errors);
            println!("   致命错误: {}", report.error_stats.fatal_errors);
            
            let recovery = &report.error_stats.error_recovery_stats;
            if recovery.successful_recoveries > 0 || recovery.failed_recoveries > 0 {
                println!("   错误恢复:");
                println!("     成功恢复: {}", recovery.successful_recoveries);
                println!("     恢复失败: {}", recovery.failed_recoveries);
                println!("     恢复成功率: {:.2}%", report.computed_metrics.recovery_success_rate * 100.0);
            }
        }
        
        // 表级指标
        if !report.top_tables.is_empty() {
            println!("\n📋 表级指标:");
            for (i, table) in report.top_tables.iter().take(5).enumerate() {
                println!("   {}. {}.{}", i + 1, table.database_name, table.table_name);
                println!("      处理行数: {}", table.rows_processed);
                println!("      处理字节: {} bytes", table.bytes_processed);
                println!("      平均行大小: {:.2} bytes", table.avg_row_size);
                println!("      操作分布: {} INSERT, {} UPDATE, {} DELETE",
                    table.operations.inserts,
                    table.operations.updates,
                    table.operations.deletes);
            }
        }
        
        // 实时指标
        println!("\n⏱️  实时指标:");
        println!("   当前行吞吐量: {:.2} rows/sec", report.realtime_metrics.current_rows_per_second);
        println!("   当前字节吞吐量: {:.2} KB/sec", 
            report.realtime_metrics.current_bytes_per_second / 1024.0);
        println!("   移动平均解析时间: {:.2}μs", 
            report.realtime_metrics.moving_avg_parse_time_ns / 1000.0);
        
        // 监控配置
        let config = parser.get_monitor().get_config();
        println!("\n⚙️  监控配置:");
        println!("   复杂度分析: {}", if config.enable_complexity_analysis { "启用" } else { "禁用" });
        println!("   表级指标: {}", if config.enable_table_metrics { "启用" } else { "禁用" });
        println!("   实时指标: {}", if config.enable_realtime_metrics { "启用" } else { "禁用" });
        println!("   采样率: {:.1}%", config.sample_rate * 100.0);
        
        // 监控持续时间
        println!("   监控持续时间: {:.2}秒", report.monitoring_duration.as_secs_f64());
        
        // 生成摘要
        println!("\n📋 人类可读摘要:");
        let summary = parser.generate_monitoring_summary();
        println!("{}", summary);
        
        println!("\n{}", "=".repeat(60));
    }
}
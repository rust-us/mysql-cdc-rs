use std::time::Duration;
use std::thread;
use common::binlog::column::column_value::SrcColumnValue;
use common::err::decode_error::ReError;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::{RowData, UpdateRowData};
use crate::row::parser::RowParser;
use crate::row::monitoring::MonitoringConfig;

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// 创建测试用的表映射事件
    fn create_test_table_map(_table_id: u64, table_name: &str, _database_name: &str) -> TableMapEvent {
        let mut table_map = TableMapEvent::default();
        table_map.set_table_name(table_name.to_string());
        // 注意：由于database_name字段是私有的，我们使用默认值
        // 在实际使用中，应该通过适当的构造函数或方法设置
        table_map
    }

    /// 创建测试用的行数据
    fn create_test_row_data(row_id: u32, name: &str, age: Option<u32>) -> RowData {
        RowData::new_with_cells(vec![
            Some(SrcColumnValue::Int(row_id)),
            Some(SrcColumnValue::String(name.to_string())),
            age.map(SrcColumnValue::Int),
            Some(SrcColumnValue::Blob(vec![1, 2, 3, 4, 5])), // 测试LOB数据
        ])
    }

    /// 模拟解析操作的测试
    #[test]
    fn test_comprehensive_monitoring() {
        println!("\n=== 开始综合监控测试 ===");
        
        // 创建带自定义监控配置的解析器
        let config = MonitoringConfig {
            enable_complexity_analysis: true,
            enable_table_metrics: true,
            enable_realtime_metrics: true,
            sample_rate: 1.0, // 监控所有行
            max_tracked_tables: 100,
            moving_average_window: 50,
            memory_tracking_interval_ms: 100,
        };
        
        let mut parser = RowParser::new_with_monitoring(1000, config);
        println!("✅ 创建解析器完成，监控配置:");
        println!("   - 复杂度分析: 启用");
        println!("   - 表级别指标: 启用");
        println!("   - 实时指标: 启用");
        println!("   - 采样率: 100%");

        // 注册测试表
        let table_map_users = create_test_table_map(1, "users", "test_db");
        let table_map_orders = create_test_table_map(2, "orders", "test_db");
        
        parser.register_table_map(1, table_map_users.clone()).unwrap();
        parser.register_table_map(2, table_map_orders.clone()).unwrap();
        println!("✅ 注册测试表完成");

        // 模拟INSERT操作
        println!("\n--- 模拟INSERT操作 ---");
        let insert_rows = vec![
            create_test_row_data(1, "Alice", Some(25)),
            create_test_row_data(2, "Bob", Some(30)),
            create_test_row_data(3, "Charlie", None), // 包含NULL值
            create_test_row_data(4, "Diana", Some(28)),
            create_test_row_data(5, "Eve", Some(35)),
        ];
        
        for (i, row) in insert_rows.iter().enumerate() {
            // 模拟行解析监控
            parser.get_monitor_mut().record_row_parsed(
                &table_map_users, 
                row, 
                Duration::from_micros(10 + i as u64 * 2), // 模拟不同的解析时间
                50 + i as u64 * 10 // 模拟不同的字节数
            );
        }
        
        parser.get_monitor_mut().record_insert_operation(&table_map_users, &insert_rows);
        println!("✅ 记录了 {} 行INSERT操作", insert_rows.len());

        // 模拟UPDATE操作
        println!("\n--- 模拟UPDATE操作 ---");
        let update_operations = vec![
            (
                create_test_row_data(1, "Alice", Some(25)),
                create_test_row_data(1, "Alice Smith", Some(26))
            ),
            (
                create_test_row_data(2, "Bob", Some(30)),
                create_test_row_data(2, "Bob Johnson", Some(30))
            ),
        ];
        
        let mut update_rows = Vec::new();
        for (before, after) in update_operations {
            // 模拟行解析监控
            parser.get_monitor_mut().record_row_parsed(
                &table_map_users, 
                &before, 
                Duration::from_micros(15),
                60
            );
            parser.get_monitor_mut().record_row_parsed(
                &table_map_users, 
                &after, 
                Duration::from_micros(18),
                65
            );
            
            update_rows.push(UpdateRowData::new_with_difference_detection(before, after));
        }
        
        parser.get_monitor_mut().record_update_operation(&table_map_users, &update_rows);
        println!("✅ 记录了 {} 行UPDATE操作", update_rows.len());

        // 模拟DELETE操作
        println!("\n--- 模拟DELETE操作 ---");
        let delete_rows = vec![
            create_test_row_data(5, "Eve", Some(35)),
        ];
        
        for row in &delete_rows {
            parser.get_monitor_mut().record_row_parsed(
                &table_map_users, 
                row, 
                Duration::from_micros(8),
                45
            );
        }
        
        parser.get_monitor_mut().record_delete_operation(&table_map_users, &delete_rows);
        println!("✅ 记录了 {} 行DELETE操作", delete_rows.len());

        // 模拟另一个表的操作
        println!("\n--- 模拟orders表操作 ---");
        let order_rows = vec![
            RowData::new_with_cells(vec![
                Some(SrcColumnValue::Int(101)),
                Some(SrcColumnValue::Int(1)), // user_id
                Some(SrcColumnValue::String("Product A".to_string())),
                Some(SrcColumnValue::Double(99.99)),
            ]),
            RowData::new_with_cells(vec![
                Some(SrcColumnValue::Int(102)),
                Some(SrcColumnValue::Int(2)), // user_id
                Some(SrcColumnValue::String("Product B".to_string())),
                Some(SrcColumnValue::Double(149.99)),
            ]),
        ];
        
        for row in &order_rows {
            parser.get_monitor_mut().record_row_parsed(
                &table_map_orders, 
                row, 
                Duration::from_micros(12),
                80
            );
        }
        
        parser.get_monitor_mut().record_insert_operation(&table_map_orders, &order_rows);
        println!("✅ 记录了 {} 行orders表操作", order_rows.len());

        // 模拟缓存操作
        println!("\n--- 模拟缓存操作 ---");
        for _ in 0..10 {
            parser.get_monitor_mut().record_cache_hit();
        }
        for _ in 0..3 {
            parser.get_monitor_mut().record_cache_miss();
        }
        println!("✅ 记录了缓存操作: 10次命中, 3次未命中");

        // 模拟错误情况
        println!("\n--- 模拟错误情况 ---");
        let errors = vec![
            ReError::String("Parse error: invalid column type".to_string()),
            ReError::String("Memory allocation failed".to_string()),
            ReError::IoError(std::io::ErrorKind::UnexpectedEof.into()),
        ];
        
        for (i, error) in errors.iter().enumerate() {
            let is_recoverable = i < 2; // 前两个错误是可恢复的
            parser.get_monitor_mut().record_error(error, Some(&table_map_users), is_recoverable);
            
            if is_recoverable {
                parser.get_monitor_mut().record_error_recovery(Duration::from_millis(5), 1);
            } else {
                parser.get_monitor_mut().record_error_recovery_failure();
            }
        }
        println!("✅ 记录了 {} 个错误 (2个可恢复, 1个致命)", errors.len());

        // 等待一小段时间以模拟实际运行
        thread::sleep(Duration::from_millis(100));

        // 打印详细的统计信息
        print_detailed_statistics(&parser);
        
        // 测试配置更新
        println!("\n--- 测试配置更新 ---");
        let new_config = MonitoringConfig {
            enable_complexity_analysis: false,
            sample_rate: 0.5,
            ..parser.get_monitor().get_config().clone()
        };
        parser.update_monitoring_config(new_config);
        println!("✅ 更新监控配置: 关闭复杂度分析, 采样率50%");

        // 测试统计重置
        println!("\n--- 测试统计重置 ---");
        let before_reset = parser.get_statistics_report();
        parser.reset_monitoring_statistics();
        let after_reset = parser.get_statistics_report();
        
        println!("重置前总行数: {}", before_reset.basic_stats.total_rows_parsed);
        println!("重置后总行数: {}", after_reset.basic_stats.total_rows_parsed);
        assert_eq!(after_reset.basic_stats.total_rows_parsed, 0);
        println!("✅ 统计重置成功");

        println!("\n=== 综合监控测试完成 ===");
    }

    /// 打印详细的统计信息
    fn print_detailed_statistics(parser: &RowParser) {
        println!("\n{}", "=".repeat(60));
        println!("                    详细统计报告");
        println!("{}", "=".repeat(60));

        // 生成并打印摘要
        let summary = parser.generate_monitoring_summary();
        println!("\n📊 统计摘要:");
        println!("{}", summary);

        // 获取详细报告
        let report = parser.get_statistics_report();

        // 基础统计
        println!("\n📈 基础统计:");
        println!("   总解析行数: {}", report.basic_stats.total_rows_parsed);
        println!("   总处理字节: {} bytes ({:.2} KB)", 
            report.basic_stats.total_bytes_processed,
            report.basic_stats.total_bytes_processed as f64 / 1024.0);
        println!("   总解析时间: {:.2} ms", 
            report.basic_stats.total_parse_time_ns as f64 / 1_000_000.0);
        println!("   内存分配次数: {}", report.basic_stats.memory_allocations);

        // 操作统计
        println!("\n🔄 操作统计:");
        println!("   INSERT操作: {}", report.basic_stats.insert_operations);
        println!("   UPDATE操作: {}", report.basic_stats.update_operations);
        println!("   DELETE操作: {}", report.basic_stats.delete_operations);

        // 缓存统计
        println!("\n💾 缓存统计:");
        println!("   缓存命中: {}", report.basic_stats.cache_hits);
        println!("   缓存未命中: {}", report.basic_stats.cache_misses);
        println!("   命中率: {:.2}%", report.computed_metrics.cache_hit_ratio * 100.0);

        // 性能指标
        println!("\n⚡ 性能指标:");
        println!("   平均解析时间: {:.2} μs", 
            report.computed_metrics.average_parse_time_ns / 1000.0);
        println!("   整体吞吐量: {:.2} rows/sec", 
            report.computed_metrics.overall_throughput_rows_per_second);
        println!("   字节吞吐量: {:.2} KB/sec", 
            report.computed_metrics.overall_throughput_bytes_per_second / 1024.0);

        // 复杂度分析
        if report.complexity_stats.max_columns_per_row > 0 {
            println!("\n🔍 复杂度分析:");
            println!("   平均行大小: {:.2} bytes", report.complexity_stats.avg_row_size_bytes);
            println!("   最小行大小: {} bytes", report.complexity_stats.min_row_size_bytes);
            println!("   最大行大小: {} bytes", report.complexity_stats.max_row_size_bytes);
            println!("   平均列数: {:.2}", report.complexity_stats.avg_columns_per_row);
            println!("   最大列数: {}", report.complexity_stats.max_columns_per_row);

            // 空值统计
            println!("\n   空值统计:");
            println!("     总空值数: {}", report.complexity_stats.null_value_stats.total_null_values);
            println!("     空值百分比: {:.2}%", report.complexity_stats.null_value_stats.null_percentage);

            // LOB统计
            if report.complexity_stats.lob_stats.lob_columns_processed > 0 {
                println!("\n   LOB统计:");
                println!("     LOB列数: {}", report.complexity_stats.lob_stats.lob_columns_processed);
                println!("     总LOB大小: {} bytes", report.complexity_stats.lob_stats.total_lob_size_bytes);
                println!("     平均LOB大小: {:.2} bytes", report.complexity_stats.lob_stats.avg_lob_size_bytes);
                println!("     最大LOB大小: {} bytes", report.complexity_stats.lob_stats.max_lob_size_bytes);
            }

            // 行大小分布
            if !report.complexity_stats.row_size_distribution.is_empty() {
                println!("\n   行大小分布:");
                for (range, count) in &report.complexity_stats.row_size_distribution {
                    println!("     {}: {} 行", range, count);
                }
            }

            // 列类型分布
            if !report.complexity_stats.column_type_distribution.is_empty() {
                println!("\n   列类型分布:");
                for (col_type, count) in &report.complexity_stats.column_type_distribution {
                    println!("     {}: {} 个", col_type, count);
                }
            }

            // 更新复杂度
            if report.complexity_stats.update_complexity.sparse_updates > 0 || 
               report.complexity_stats.update_complexity.full_updates > 0 {
                println!("\n   更新复杂度:");
                println!("     平均变更百分比: {:.2}%", 
                    report.complexity_stats.update_complexity.avg_change_percentage);
                println!("     稀疏更新: {}", report.complexity_stats.update_complexity.sparse_updates);
                println!("     完整更新: {}", report.complexity_stats.update_complexity.full_updates);
            }
        }

        // 错误统计
        if report.error_stats.total_errors > 0 {
            println!("\n❌ 错误统计:");
            println!("   总错误数: {}", report.error_stats.total_errors);
            println!("   错误率: {:.4}%", report.computed_metrics.error_rate * 100.0);
            println!("   可恢复错误: {}", report.error_stats.recoverable_errors);
            println!("   致命错误: {}", report.error_stats.fatal_errors);
            println!("   解析错误: {}", report.error_stats.parse_errors);
            println!("   IO错误: {}", report.error_stats.io_errors);
            println!("   内存错误: {}", report.error_stats.memory_errors);

            // 错误恢复统计
            let recovery = &report.error_stats.error_recovery_stats;
            if recovery.successful_recoveries > 0 || recovery.failed_recoveries > 0 {
                println!("\n   错误恢复:");
                println!("     成功恢复: {}", recovery.successful_recoveries);
                println!("     恢复失败: {}", recovery.failed_recoveries);
                println!("     恢复成功率: {:.2}%", report.computed_metrics.recovery_success_rate * 100.0);
                println!("     平均恢复时间: {:.2} ms", recovery.avg_recovery_time_ns as f64 / 1_000_000.0);
                println!("     跳过行数: {}", recovery.rows_skipped);
            }

            // 按类型分组的错误
            if !report.error_stats.errors_by_type.is_empty() {
                println!("\n   按类型分组:");
                for (error_type, count) in &report.error_stats.errors_by_type {
                    println!("     {}: {} 次", error_type, count);
                }
            }

            // 按表分组的错误
            if !report.error_stats.errors_by_table.is_empty() {
                println!("\n   按表分组:");
                for (table, count) in &report.error_stats.errors_by_table {
                    println!("     {}: {} 次", table, count);
                }
            }
        }

        // 表级别指标
        if !report.top_tables.is_empty() {
            println!("\n📋 表级别指标 (按活跃度排序):");
            for (i, table) in report.top_tables.iter().take(10).enumerate() {
                println!("   {}. {}.{}", i + 1, table.database_name, table.table_name);
                println!("      处理行数: {}", table.rows_processed);
                println!("      处理字节: {} bytes", table.bytes_processed);
                println!("      平均行大小: {:.2} bytes", table.avg_row_size);
                println!("      处理时间: {:.2} ms", table.processing_time_ns as f64 / 1_000_000.0);
                println!("      操作分布: {} INSERT, {} UPDATE, {} DELETE",
                    table.operations.inserts,
                    table.operations.updates,
                    table.operations.deletes);
                if table.error_count > 0 {
                    println!("      错误数: {}", table.error_count);
                }
                if let Some(last_processed) = table.last_processed {
                    println!("      最后处理: {:?}秒前", 
                        std::time::Instant::now().duration_since(last_processed).as_secs());
                }
                println!();
            }
        }

        // 实时指标
        println!("⏱️  实时指标:");
        println!("   当前行吞吐量: {:.2} rows/sec", report.realtime_metrics.current_rows_per_second);
        println!("   当前字节吞吐量: {:.2} bytes/sec", report.realtime_metrics.current_bytes_per_second);
        println!("   移动平均解析时间: {:.2} μs", 
            report.realtime_metrics.moving_avg_parse_time_ns / 1000.0);
        println!("   峰值内存使用: {} bytes", report.realtime_metrics.peak_memory_usage_bytes);
        println!("   当前内存使用: {} bytes", report.realtime_metrics.current_memory_usage_bytes);

        // 监控配置
        let config = parser.get_monitor().get_config();
        println!("\n⚙️  监控配置:");
        println!("   复杂度分析: {}", if config.enable_complexity_analysis { "启用" } else { "禁用" });
        println!("   表级别指标: {}", if config.enable_table_metrics { "启用" } else { "禁用" });
        println!("   实时指标: {}", if config.enable_realtime_metrics { "启用" } else { "禁用" });
        println!("   采样率: {:.1}%", config.sample_rate * 100.0);
        println!("   最大跟踪表数: {}", config.max_tracked_tables);
        println!("   移动平均窗口: {}", config.moving_average_window);

        // 监控持续时间
        println!("\n⏰ 监控持续时间: {:.2}秒", report.monitoring_duration.as_secs_f64());

        println!("\n{}", "=".repeat(60));
    }

    /// 测试完整的解析器API
    #[test]
    fn test_complete_parser_api() {
        println!("\n=== 完整解析器API测试 ===");
        
        // 测试不同的构造方法
        println!("\n--- 测试构造方法 ---");
        let default_parser = RowParser::with_default_cache();
        println!("✅ 默认解析器创建成功，缓存大小: {}", default_parser.cache_size().unwrap());
        
        let custom_parser = RowParser::new(500);
        println!("✅ 自定义解析器创建成功，缓存大小: {}", custom_parser.cache_size().unwrap());
        
        let config = MonitoringConfig {
            enable_complexity_analysis: true,
            enable_table_metrics: true,
            enable_realtime_metrics: true,
            sample_rate: 0.5,
            max_tracked_tables: 50,
            moving_average_window: 20,
            memory_tracking_interval_ms: 500,
        };
        let monitoring_parser = RowParser::new_with_monitoring(300, config);
        println!("✅ 监控解析器创建成功，缓存大小: {}", monitoring_parser.cache_size().unwrap());
        
        let legacy_parser = RowParser::new_legacy(200);
        println!("✅ 兼容模式解析器创建成功，缓存大小: {}", legacy_parser.cache_size().unwrap());
        
        // 测试优化开关
        println!("\n--- 测试优化开关 ---");
        let mut opt_parser = RowParser::new(100);
        opt_parser.set_optimizations_enabled(false);
        println!("✅ 禁用优化成功");
        opt_parser.set_optimizations_enabled(true);
        println!("✅ 启用优化成功");
        
        // 测试表映射管理
        println!("\n--- 测试表映射管理 ---");
        let mut parser = RowParser::new(100);
        let table_map1 = create_test_table_map(1, "users", "test_db");
        let table_map2 = create_test_table_map(2, "orders", "test_db");
        
        parser.register_table_map(1, table_map1.clone()).unwrap();
        parser.register_table_map(2, table_map2.clone()).unwrap();
        println!("✅ 注册表映射成功，缓存大小: {}", parser.cache_size().unwrap());
        
        let retrieved = parser.get_table_map(1).unwrap();
        assert!(retrieved.is_some());
        println!("✅ 获取表映射成功");
        
        let retrieved_with_stats = parser.get_table_map_with_stats(1).unwrap();
        assert!(retrieved_with_stats.is_some());
        println!("✅ 获取表映射(带统计)成功");
        
        // 测试监控配置更新
        println!("\n--- 测试监控配置更新 ---");
        let original_config = parser.get_monitor().get_config();
        println!("原始配置 - 采样率: {:.1}%", original_config.sample_rate * 100.0);
        
        let new_config = MonitoringConfig {
            sample_rate: 0.2,
            enable_complexity_analysis: false,
            ..original_config.clone()
        };
        parser.update_monitoring_config(new_config);
        
        let updated_config = parser.get_monitor().get_config();
        println!("更新配置 - 采样率: {:.1}%", updated_config.sample_rate * 100.0);
        println!("更新配置 - 复杂度分析: {}", updated_config.enable_complexity_analysis);
        
        // 测试统计重置
        println!("\n--- 测试统计重置 ---");
        // 添加一些测试数据
        let test_row = create_test_row_data(1, "test", Some(100));
        parser.get_monitor_mut().record_row_parsed(&table_map1, &test_row, Duration::from_micros(10), 50);
        
        let before_reset = parser.get_statistics_report();
        println!("重置前行数: {}", before_reset.basic_stats.total_rows_parsed);
        
        parser.reset_monitoring_statistics();
        let after_reset = parser.get_statistics_report();
        println!("重置后行数: {}", after_reset.basic_stats.total_rows_parsed);
        assert_eq!(after_reset.basic_stats.total_rows_parsed, 0);
        
        // 测试缓存清理
        println!("\n--- 测试缓存清理 ---");
        parser.clear_cache().unwrap();
        println!("✅ 缓存清理成功，当前大小: {}", parser.cache_size().unwrap());
        
        println!("✅ 完整API测试完成");
    }

    /// 测试性能告警功能
    #[test]
    fn test_performance_alerts() {
        println!("\n=== 性能告警测试 ===");
        
        let mut parser = RowParser::with_default_cache();
        let table_map = create_test_table_map(1, "test_table", "test_db");
        parser.register_table_map(1, table_map.clone()).unwrap();

        // 模拟高错误率场景
        for i in 0..100 {
            let error = ReError::String(format!("Test error {}", i));
            parser.get_monitor_mut().record_error(&error, Some(&table_map), false);
        }

        // 模拟低缓存命中率
        for _ in 0..20 {
            parser.get_monitor_mut().record_cache_miss();
        }
        for _ in 0..5 {
            parser.get_monitor_mut().record_cache_hit();
        }

        check_performance_alerts(&parser);
        println!("✅ 性能告警测试完成");
    }

    /// 测试不同解析方法的性能对比
    #[test]
    fn test_parsing_methods_comparison() {
        println!("\n=== 解析方法性能对比测试 ===");
        
        // 创建优化和非优化解析器
        let mut optimized_parser = RowParser::new(500);
        optimized_parser.set_optimizations_enabled(true);
        
        let mut legacy_parser = RowParser::new_legacy(500);
        
        let table_map = create_test_table_map(1, "performance_test", "test_db");
        optimized_parser.register_table_map(1, table_map.clone()).unwrap();
        legacy_parser.register_table_map(1, table_map.clone()).unwrap();
        
        // 模拟解析操作
        let test_rows = vec![
            create_test_row_data(1, "Alice", Some(25)),
            create_test_row_data(2, "Bob", Some(30)),
            create_test_row_data(3, "Charlie", None),
        ];
        
        println!("\n--- 优化解析器性能 ---");
        let start_time = std::time::Instant::now();
        for (i, row) in test_rows.iter().enumerate() {
            optimized_parser.get_monitor_mut().record_row_parsed(
                &table_map, 
                row, 
                Duration::from_micros(8 + i as u64), 
                60 + i as u64 * 5
            );
        }
        let optimized_duration = start_time.elapsed();
        
        println!("--- 兼容解析器性能 ---");
        let start_time = std::time::Instant::now();
        for (i, row) in test_rows.iter().enumerate() {
            legacy_parser.get_monitor_mut().record_row_parsed(
                &table_map, 
                row, 
                Duration::from_micros(12 + i as u64 * 2), 
                60 + i as u64 * 5
            );
        }
        let legacy_duration = start_time.elapsed();
        
        // 输出性能对比
        println!("\n📊 性能对比结果:");
        println!("   优化解析器耗时: {:.2}μs", optimized_duration.as_micros());
        println!("   兼容解析器耗时: {:.2}μs", legacy_duration.as_micros());
        
        let optimized_report = optimized_parser.get_statistics_report();
        let legacy_report = legacy_parser.get_statistics_report();
        
        println!("   优化解析器平均时间: {:.2}μs", 
            optimized_report.computed_metrics.average_parse_time_ns / 1000.0);
        println!("   兼容解析器平均时间: {:.2}μs", 
            legacy_report.computed_metrics.average_parse_time_ns / 1000.0);
        
        println!("✅ 性能对比测试完成");
    }

    /// 测试监控配置的影响
    #[test]
    fn test_monitoring_configuration_impact() {
        println!("\n=== 监控配置影响测试 ===");
        
        // 测试不同采样率的影响
        let configs = vec![
            ("100%采样", MonitoringConfig {
                sample_rate: 1.0,
                enable_complexity_analysis: true,
                enable_table_metrics: true,
                enable_realtime_metrics: true,
                ..Default::default()
            }),
            ("50%采样", MonitoringConfig {
                sample_rate: 0.5,
                enable_complexity_analysis: true,
                enable_table_metrics: true,
                enable_realtime_metrics: true,
                ..Default::default()
            }),
            ("10%采样", MonitoringConfig {
                sample_rate: 0.1,
                enable_complexity_analysis: true,
                enable_table_metrics: true,
                enable_realtime_metrics: true,
                ..Default::default()
            }),
            ("最小监控", MonitoringConfig {
                sample_rate: 0.01,
                enable_complexity_analysis: false,
                enable_table_metrics: false,
                enable_realtime_metrics: true,
                ..Default::default()
            }),
        ];
        
        for (name, config) in configs {
            println!("\n--- 测试配置: {} ---", name);
            let mut parser = RowParser::new_with_monitoring(500, config);
            let table_map = create_test_table_map(1, "config_test", "test_db");
            parser.register_table_map(1, table_map.clone()).unwrap();
            
            // 模拟大量数据处理
            let start_time = std::time::Instant::now();
            for i in 0..100 {
                let row = create_test_row_data(i, &format!("user_{}", i), Some(20 + i % 50));
                parser.get_monitor_mut().record_row_parsed(
                    &table_map, 
                    &row, 
                    Duration::from_micros(10), 
                    80
                );
            }
            let processing_time = start_time.elapsed();
            
            let report = parser.get_statistics_report();
            println!("   处理时间: {:.2}ms", processing_time.as_millis());
            println!("   监控到的行数: {}", report.basic_stats.total_rows_parsed);
            println!("   平均解析时间: {:.2}μs", 
                report.computed_metrics.average_parse_time_ns / 1000.0);
            
            if report.complexity_stats.max_columns_per_row > 0 {
                println!("   复杂度分析: 启用 (平均行大小: {:.2} bytes)", 
                    report.complexity_stats.avg_row_size_bytes);
            } else {
                println!("   复杂度分析: 禁用");
            }
        }
        
        println!("\n✅ 监控配置影响测试完成");
    }

    /// 检查性能告警
    fn check_performance_alerts(parser: &RowParser) {
        let report = parser.get_statistics_report();
        
        println!("\n🚨 性能告警检查:");
        
        // 错误率告警
        if report.computed_metrics.error_rate > 0.01 { // 1%
            println!("   ❌ 错误率过高: {:.2}%", report.computed_metrics.error_rate * 100.0);
        } else {
            println!("   ✅ 错误率正常: {:.4}%", report.computed_metrics.error_rate * 100.0);
        }
        
        // 吞吐量告警
        if report.computed_metrics.overall_throughput_rows_per_second < 100.0 {
            println!("   ⚠️  吞吐量较低: {:.2} rows/sec", 
                report.computed_metrics.overall_throughput_rows_per_second);
        } else {
            println!("   ✅ 吞吐量正常: {:.2} rows/sec", 
                report.computed_metrics.overall_throughput_rows_per_second);
        }
        
        // 缓存命中率告警
        if report.computed_metrics.cache_hit_ratio < 0.8 {
            println!("   ❌ 缓存命中率过低: {:.2}%", 
                report.computed_metrics.cache_hit_ratio * 100.0);
        } else {
            println!("   ✅ 缓存命中率正常: {:.2}%", 
                report.computed_metrics.cache_hit_ratio * 100.0);
        }
        
        // 平均解析时间告警
        if report.computed_metrics.average_parse_time_ns > 100_000.0 { // 100μs
            println!("   ⚠️  解析时间较长: {:.2} μs", 
                report.computed_metrics.average_parse_time_ns / 1000.0);
        } else {
            println!("   ✅ 解析时间正常: {:.2} μs", 
                report.computed_metrics.average_parse_time_ns / 1000.0);
        }
    }

    /// 测试配置优化建议
    #[test]
    fn test_optimization_suggestions() {
        println!("\n=== 优化建议测试 ===");
        
        let mut parser = RowParser::with_default_cache();
        let table_map = create_test_table_map(1, "large_table", "test_db");
        parser.register_table_map(1, table_map.clone()).unwrap();

        // 模拟大行数据
        let large_row = RowData::new_with_cells(vec![
            Some(SrcColumnValue::Blob(vec![0u8; 2000])), // 2KB的BLOB
            Some(SrcColumnValue::String("x".repeat(500))), // 500字符的字符串
            None, None, None, None, None, // 多个NULL值
        ]);

        for _i in 0..10 {
            parser.get_monitor_mut().record_row_parsed(
                &table_map, 
                &large_row, 
                Duration::from_micros(50), 
                2500 // 大行大小
            );
        }

        generate_optimization_suggestions(&parser);
        println!("✅ 优化建议测试完成");
    }

    /// 生成优化建议
    fn generate_optimization_suggestions(parser: &RowParser) {
        let report = parser.get_statistics_report();
        
        println!("\n💡 优化建议:");
        
        // 基于复杂度统计的建议
        if report.complexity_stats.avg_row_size_bytes > 1000.0 {
            println!("   📏 行数据较大({:.2} bytes)，建议:", report.complexity_stats.avg_row_size_bytes);
            println!("      - 增加解析器缓存大小");
            println!("      - 考虑使用流式处理");
            println!("      - 优化数据结构以减少内存占用");
        }
        
        if report.complexity_stats.null_value_stats.null_percentage > 50.0 {
            println!("   🕳️  空值比例较高({:.2}%)，建议:", report.complexity_stats.null_value_stats.null_percentage);
            println!("      - 优化数据库schema设计");
            println!("      - 使用稀疏存储格式");
            println!("      - 考虑数据压缩");
        }
        
        // 基于性能的建议
        if report.computed_metrics.cache_hit_ratio < 0.7 {
            println!("   💾 缓存命中率较低({:.2}%)，建议:", 
                report.computed_metrics.cache_hit_ratio * 100.0);
            println!("      - 增加缓存大小");
            println!("      - 调整缓存策略");
            println!("      - 检查表访问模式");
        }
        
        // 基于错误率的建议
        if report.computed_metrics.error_rate > 0.005 {
            println!("   ❌ 错误率较高({:.4}%)，建议:", 
                report.computed_metrics.error_rate * 100.0);
            println!("      - 检查数据质量");
            println!("      - 验证解析逻辑");
            println!("      - 增强错误处理");
        }
        
        // 基于LOB统计的建议
        if report.complexity_stats.lob_stats.avg_lob_size_bytes > 10000.0 {
            println!("   📦 LOB数据较大({:.2} bytes)，建议:", 
                report.complexity_stats.lob_stats.avg_lob_size_bytes);
            println!("      - 使用流式LOB处理");
            println!("      - 考虑LOB数据压缩");
            println!("      - 分离LOB存储");
        }
        
        // 基于更新模式的建议
        let update_stats = &report.complexity_stats.update_complexity;
        if update_stats.sparse_updates > 0 && update_stats.full_updates > 0 {
            let sparse_ratio = update_stats.sparse_updates as f64 / 
                (update_stats.sparse_updates + update_stats.full_updates) as f64;
            
            if sparse_ratio > 0.8 {
                println!("   🔄 更新模式以稀疏更新为主({:.1}%)，建议:", sparse_ratio * 100.0);
                println!("      - 使用增量更新优化");
                println!("      - 启用字段级差异检测");
                println!("      - 考虑列式存储");
            }
        }
        
        if report.basic_stats.total_rows_parsed == 0 {
            println!("   ⚠️  没有监控到数据，检查:");
            println!("      - 采样率配置是否过低");
            println!("      - 监控是否正确启用");
            println!("      - 数据源是否正常");
        }
    }

    /// 测试数据导出功能
    #[test]
    fn test_data_export() {
        println!("\n=== 数据导出测试 ===");
        
        let mut parser = RowParser::with_default_cache();
        let table_map = create_test_table_map(1, "export_test", "test_db");
        parser.register_table_map(1, table_map.clone()).unwrap();

        // 添加一些测试数据
        let test_row = create_test_row_data(1, "test", Some(25));
        parser.get_monitor_mut().record_row_parsed(
            &table_map, 
            &test_row, 
            Duration::from_micros(10), 
            100
        );

        // 测试数据导出（模拟）
        let report = parser.get_statistics_report();
        // 创建简化的JSON格式数据
        let json_data = format!(
            "{{\"total_rows_parsed\":{},\"total_bytes_processed\":{},\"insert_operations\":{},\"update_operations\":{},\"delete_operations\":{},\"total_errors\":{},\"cache_hit_ratio\":{:.4},\"average_parse_time_ns\":{:.2}}}",
            report.basic_stats.total_rows_parsed,
            report.basic_stats.total_bytes_processed,
            report.basic_stats.insert_operations,
            report.basic_stats.update_operations,
            report.basic_stats.delete_operations,
            report.error_stats.total_errors,
            report.computed_metrics.cache_hit_ratio,
            report.computed_metrics.average_parse_time_ns
        );
        println!("✅ JSON导出成功，大小: {} bytes", json_data.len());

        // 测试摘要导出
        let summary = parser.generate_monitoring_summary();
        println!("✅ 摘要导出成功，长度: {} 字符", summary.len());
        
        println!("✅ 数据导出测试完成");
    }

    /// 完整的统计数据展示测试
    #[test]
    fn test_comprehensive_statistics_display() {
        println!("\n{}", "=".repeat(80));
        println!("                    完整统计数据展示测试");
        println!("{}", "=".repeat(80));
        
        // 创建具有完整监控的解析器
        let config = MonitoringConfig {
            enable_complexity_analysis: true,
            enable_table_metrics: true,
            enable_realtime_metrics: true,
            sample_rate: 1.0,
            max_tracked_tables: 100,
            moving_average_window: 20,
            memory_tracking_interval_ms: 100,
        };
        let mut parser = RowParser::new_with_monitoring(1000, config);
        
        // 创建多个测试表
        let tables = vec![
            ("users", "user_db"),
            ("orders", "order_db"),
            ("products", "product_db"),
            ("logs", "system_db"),
        ];
        
        for (i, (table_name, db_name)) in tables.iter().enumerate() {
            let table_map = create_test_table_map(i as u64 + 1, table_name, db_name);
            parser.register_table_map(i as u64 + 1, table_map).unwrap();
        }
        
        println!("✅ 创建了 {} 个测试表", tables.len());
        
        // 模拟复杂的数据处理场景
        simulate_complex_data_processing(&mut parser);
        
        // 等待一段时间以获得更真实的时间统计
        thread::sleep(Duration::from_millis(50));
        
        // 输出所有统计数据
        display_all_statistics(&parser);
        
        // 测试不同格式的输出
        test_different_output_formats(&parser);
        
        println!("\n{}", "=".repeat(80));
        println!("✅ 完整统计数据展示测试完成");
        println!("{}", "=".repeat(80));
    }
    
    /// 模拟复杂的数据处理场景
    fn simulate_complex_data_processing(parser: &mut RowParser) {
        println!("\n📊 模拟复杂数据处理场景...");
        
        // 获取表映射
        let user_table = parser.get_table_map(1).unwrap().unwrap();
        let order_table = parser.get_table_map(2).unwrap().unwrap();
        let product_table = parser.get_table_map(3).unwrap().unwrap();
        let log_table = parser.get_table_map(4).unwrap().unwrap();
        
        // 模拟用户表的大量INSERT操作
        let user_rows = (1..=50).map(|i| {
            if i % 10 == 0 {
                // 创建一些大行数据
                RowData::new_with_cells(vec![
                    Some(SrcColumnValue::Int(i)),
                    Some(SrcColumnValue::String(format!("user_with_very_long_name_{}", i))),
                    Some(SrcColumnValue::Blob(vec![0u8; 1000])), // 1KB BLOB
                    Some(SrcColumnValue::String("A".repeat(500))), // 500字符字符串
                    None, None, None, // 一些NULL值
                ])
            } else {
                create_test_row_data(i, &format!("user_{}", i), Some(20 + i % 50))
            }
        }).collect::<Vec<_>>();
        
        for (i, row) in user_rows.iter().enumerate() {
            let parse_time = Duration::from_micros(8 + (i % 10) as u64 * 2);
            let bytes = if i % 10 == 0 { 1600 } else { 80 + i as u64 * 2 };
            parser.get_monitor_mut().record_row_parsed(&user_table, row, parse_time, bytes);
        }
        parser.get_monitor_mut().record_insert_operation(&user_table, &user_rows);
        
        // 模拟订单表的UPDATE操作
        let order_updates = (1..=20).map(|i| {
            let before = RowData::new_with_cells(vec![
                Some(SrcColumnValue::Int(i)),
                Some(SrcColumnValue::Int(i % 10 + 1)), // user_id
                Some(SrcColumnValue::String(format!("Product {}", i))),
                Some(SrcColumnValue::Double(99.99 + i as f64)),
                Some(SrcColumnValue::String("pending".to_string())),
            ]);
            let after = RowData::new_with_cells(vec![
                Some(SrcColumnValue::Int(i)),
                Some(SrcColumnValue::Int(i % 10 + 1)), // user_id
                Some(SrcColumnValue::String(format!("Product {}", i))),
                Some(SrcColumnValue::Double(99.99 + i as f64)),
                Some(SrcColumnValue::String("completed".to_string())), // 状态更新
            ]);
            UpdateRowData::new_with_difference_detection(before, after)
        }).collect::<Vec<_>>();
        
        for update in &order_updates {
            parser.get_monitor_mut().record_row_parsed(&order_table, &update.before_update, Duration::from_micros(12), 120);
            parser.get_monitor_mut().record_row_parsed(&order_table, &update.after_update, Duration::from_micros(14), 125);
        }
        parser.get_monitor_mut().record_update_operation(&order_table, &order_updates);
        
        // 模拟产品表的DELETE操作
        let deleted_products = (1..=5).map(|i| {
            RowData::new_with_cells(vec![
                Some(SrcColumnValue::Int(i)),
                Some(SrcColumnValue::String(format!("Discontinued Product {}", i))),
                Some(SrcColumnValue::Double(0.0)), // 价格设为0
                None, // 描述为空
            ])
        }).collect::<Vec<_>>();
        
        for product in &deleted_products {
            parser.get_monitor_mut().record_row_parsed(&product_table, product, Duration::from_micros(6), 60);
        }
        parser.get_monitor_mut().record_delete_operation(&product_table, &deleted_products);
        
        // 模拟日志表的高频小数据INSERT
        let log_entries = (1..=100).map(|i| {
            RowData::new_with_cells(vec![
                Some(SrcColumnValue::Int(i)),
                Some(SrcColumnValue::String(format!("INFO: Operation {} completed", i))),
                Some(SrcColumnValue::String("system".to_string())),
                if i % 20 == 0 { None } else { Some(SrcColumnValue::String("details".to_string())) },
            ])
        }).collect::<Vec<_>>();
        
        for (i, log) in log_entries.iter().enumerate() {
            let parse_time = Duration::from_micros(3 + (i % 5) as u64);
            parser.get_monitor_mut().record_row_parsed(&log_table, log, parse_time, 40 + i as u64);
        }
        parser.get_monitor_mut().record_insert_operation(&log_table, &log_entries);
        
        // 模拟缓存操作
        for _ in 0..80 {
            parser.get_monitor_mut().record_cache_hit();
        }
        for _ in 0..20 {
            parser.get_monitor_mut().record_cache_miss();
        }
        
        // 模拟各种错误
        let errors = vec![
            (ReError::String("Parse error: invalid column data".to_string()), true),
            (ReError::String("Memory allocation failed".to_string()), true),
            (ReError::String("Timeout during parsing".to_string()), true),
            (ReError::IoError(std::io::ErrorKind::UnexpectedEof.into()), false),
            (ReError::String("Fatal: corrupted data".to_string()), false),
        ];
        
        for (i, (error, recoverable)) in errors.iter().enumerate() {
            let table = match i % 4 {
                0 => &user_table,
                1 => &order_table,
                2 => &product_table,
                _ => &log_table,
            };
            
            parser.get_monitor_mut().record_error(error, Some(table), *recoverable);
            
            if *recoverable {
                parser.get_monitor_mut().record_error_recovery(Duration::from_millis(5 + i as u64), 1);
            } else {
                parser.get_monitor_mut().record_error_recovery_failure();
            }
        }
        
        println!("✅ 复杂数据处理场景模拟完成");
        println!("   - 用户表: {} 行INSERT", user_rows.len());
        println!("   - 订单表: {} 行UPDATE", order_updates.len());
        println!("   - 产品表: {} 行DELETE", deleted_products.len());
        println!("   - 日志表: {} 行INSERT", log_entries.len());
        println!("   - 缓存操作: 80次命中, 20次未命中");
        println!("   - 错误模拟: {} 个错误 (3个可恢复, 2个致命)", errors.len());
    }
    
    /// 显示所有统计数据
    fn display_all_statistics(parser: &RowParser) {
        println!("\n{}", "█".repeat(80));
        println!("                        📊 完整统计数据报告");
        println!("{}", "█".repeat(80));
        
        let report = parser.get_statistics_report();
        
        // 1. 监控概览
        println!("\n🔍 监控概览:");
        println!("   监控持续时间: {:.2}秒", report.monitoring_duration.as_secs_f64());
        let config = parser.get_monitor().get_config();
        println!("   监控配置:");
        println!("     - 复杂度分析: {}", if config.enable_complexity_analysis { "✅ 启用" } else { "❌ 禁用" });
        println!("     - 表级指标: {}", if config.enable_table_metrics { "✅ 启用" } else { "❌ 禁用" });
        println!("     - 实时指标: {}", if config.enable_realtime_metrics { "✅ 启用" } else { "❌ 禁用" });
        println!("     - 采样率: {:.1}%", config.sample_rate * 100.0);
        println!("     - 最大跟踪表数: {}", config.max_tracked_tables);
        
        // 2. 基础统计
        println!("\n📈 基础统计数据:");
        println!("   总解析行数: {}", report.basic_stats.total_rows_parsed);
        println!("   总处理字节: {} bytes", report.basic_stats.total_bytes_processed);
        println!("   总解析时间: {:.2} ms", report.basic_stats.total_parse_time_ns as f64 / 1_000_000.0);
        println!("   内存分配次数: {}", report.basic_stats.memory_allocations);
        
        // 3. 操作统计
        println!("\n🔄 操作统计:");
        let total_ops = report.basic_stats.insert_operations + 
                       report.basic_stats.update_operations + 
                       report.basic_stats.delete_operations;
        println!("   INSERT操作: {}", report.basic_stats.insert_operations);
        println!("   UPDATE操作: {}", report.basic_stats.update_operations);
        println!("   DELETE操作: {}", report.basic_stats.delete_operations);
        println!("   总操作数: {}", total_ops);
        
        if total_ops > 0 {
            println!("   操作分布:");
            println!("     INSERT: {:.1}%", 
                report.basic_stats.insert_operations as f64 / total_ops as f64 * 100.0);
            println!("     UPDATE: {:.1}%", 
                report.basic_stats.update_operations as f64 / total_ops as f64 * 100.0);
            println!("     DELETE: {:.1}%", 
                report.basic_stats.delete_operations as f64 / total_ops as f64 * 100.0);
        }
        
        // 4. 性能指标
        println!("\n⚡ 性能指标:");
        println!("   平均解析时间: {:.2}μs", report.computed_metrics.average_parse_time_ns / 1000.0);
        println!("   行吞吐量: {:.2} rows/sec", report.computed_metrics.overall_throughput_rows_per_second);
        println!("   字节吞吐量: {:.2} KB/sec", report.computed_metrics.overall_throughput_bytes_per_second / 1024.0);
        
        // 5. 缓存统计
        println!("\n💾 缓存统计:");
        let total_cache_ops = report.basic_stats.cache_hits + report.basic_stats.cache_misses;
        println!("   缓存命中: {}", report.basic_stats.cache_hits);
        println!("   缓存未命中: {}", report.basic_stats.cache_misses);
        println!("   命中率: {:.2}%", report.computed_metrics.cache_hit_ratio * 100.0);
        
        if total_cache_ops > 0 {
            let hit_ratio = report.basic_stats.cache_hits as f64 / total_cache_ops as f64;
            println!("   缓存效率: {:.1}%", hit_ratio * 100.0);
        }
        
        // 6. 复杂度分析
        if report.complexity_stats.max_columns_per_row > 0 {
            println!("\n🔍 数据复杂度分析:");
            println!("   平均行大小: {:.2} bytes", report.complexity_stats.avg_row_size_bytes);
            println!("   最小行大小: {} bytes", report.complexity_stats.min_row_size_bytes);
            println!("   最大行大小: {} bytes", report.complexity_stats.max_row_size_bytes);
            println!("   平均列数: {:.1}", report.complexity_stats.avg_columns_per_row);
            println!("   最大列数: {}", report.complexity_stats.max_columns_per_row);
            
            // 空值统计
            println!("\n   空值统计:");
            println!("     总空值数: {}", report.complexity_stats.null_value_stats.total_null_values);
            println!("     空值百分比: {:.2}%", report.complexity_stats.null_value_stats.null_percentage);
            
            // LOB统计
            if report.complexity_stats.lob_stats.lob_columns_processed > 0 {
                println!("\n   大对象(LOB)统计:");
                println!("     LOB列数: {}", report.complexity_stats.lob_stats.lob_columns_processed);
                println!("     总LOB大小: {} bytes", report.complexity_stats.lob_stats.total_lob_size_bytes);
                println!("     平均LOB大小: {:.2} bytes", report.complexity_stats.lob_stats.avg_lob_size_bytes);
                println!("     最大LOB大小: {} bytes", report.complexity_stats.lob_stats.max_lob_size_bytes);
            }
            
            // 行大小分布
            if !report.complexity_stats.row_size_distribution.is_empty() {
                println!("\n   行大小分布:");
                for (range, count) in &report.complexity_stats.row_size_distribution {
                    println!("     {}: {} 行", range, count);
                }
            }
            
            // 列类型分布
            if !report.complexity_stats.column_type_distribution.is_empty() {
                println!("\n   列类型分布:");
                for (col_type, count) in &report.complexity_stats.column_type_distribution {
                    println!("     {}: {} 个", col_type, count);
                }
            }
        }
        
        // 7. 错误统计
        if report.error_stats.total_errors > 0 {
            println!("\n❌ 错误统计:");
            println!("   总错误数: {}", report.error_stats.total_errors);
            println!("   错误率: {:.4}%", report.computed_metrics.error_rate * 100.0);
            println!("   可恢复错误: {}", report.error_stats.recoverable_errors);
            println!("   致命错误: {}", report.error_stats.fatal_errors);
            
            // 错误恢复统计
            let recovery = &report.error_stats.error_recovery_stats;
            if recovery.successful_recoveries > 0 || recovery.failed_recoveries > 0 {
                println!("\n   错误恢复统计:");
                println!("     成功恢复: {}", recovery.successful_recoveries);
                println!("     恢复失败: {}", recovery.failed_recoveries);
                println!("     恢复成功率: {:.2}%", report.computed_metrics.recovery_success_rate * 100.0);
                println!("     平均恢复时间: {:.2}ms", recovery.avg_recovery_time_ns as f64 / 1_000_000.0);
                println!("     跳过行数: {}", recovery.rows_skipped);
            }
            
            // 错误类型分布
            if !report.error_stats.errors_by_type.is_empty() {
                println!("\n   错误类型分布:");
                for (error_type, count) in &report.error_stats.errors_by_type {
                    println!("     {}: {} 次", error_type, count);
                }
            }
        }
        
        // 8. 表级指标
        if !report.top_tables.is_empty() {
            println!("\n📋 表级指标 (按活跃度排序):");
            for (i, table) in report.top_tables.iter().take(10).enumerate() {
                println!("\n   {}. {}.{}", i + 1, table.database_name, table.table_name);
                println!("      处理行数: {}", table.rows_processed);
                println!("      处理字节: {} bytes", table.bytes_processed);
                println!("      平均行大小: {:.2} bytes", table.avg_row_size);
                println!("      处理时间: {:.2} ms", table.processing_time_ns as f64 / 1_000_000.0);
                println!("      操作分布: {} INSERT, {} UPDATE, {} DELETE",
                    table.operations.inserts,
                    table.operations.updates,
                    table.operations.deletes);
                if table.error_count > 0 {
                    println!("      错误数: {}", table.error_count);
                }
                if let Some(last_processed) = table.last_processed {
                    println!("      最后处理: {}秒前", 
                        std::time::Instant::now().duration_since(last_processed).as_secs());
                } else {
                    println!("      最后处理: 未知");
                }
            }
        }
        
        // 9. 实时指标
        println!("\n⏱️  实时指标:");
        println!("   当前行吞吐量: {:.2} rows/sec", report.realtime_metrics.current_rows_per_second);
        println!("   当前字节吞吐量: {:.2} KB/sec", report.realtime_metrics.current_bytes_per_second / 1024.0);
        println!("   移动平均解析时间: {:.2}μs", report.realtime_metrics.moving_avg_parse_time_ns / 1000.0);
        println!("   峰值内存使用: {} bytes", report.realtime_metrics.peak_memory_usage_bytes);
        println!("   当前内存使用: {} bytes", report.realtime_metrics.current_memory_usage_bytes);
        
        println!("\n{}", "█".repeat(80));
    }
    
    /// 测试不同输出格式
    fn test_different_output_formats(parser: &RowParser) {
        println!("\n📄 测试不同输出格式:");
        
        // 1. 人类可读摘要
        println!("\n--- 人类可读摘要 ---");
        let summary = parser.generate_monitoring_summary();
        println!("{}", summary);
        
        // 2. 结构化数据输出
        println!("\n--- 结构化数据 ---");
        let report = parser.get_statistics_report();
        println!("基础统计JSON格式:");
        println!("{{");
        println!("  \"total_rows_parsed\": {},", report.basic_stats.total_rows_parsed);
        println!("  \"total_bytes_processed\": {},", report.basic_stats.total_bytes_processed);
        println!("  \"insert_operations\": {},", report.basic_stats.insert_operations);
        println!("  \"update_operations\": {},", report.basic_stats.update_operations);
        println!("  \"delete_operations\": {},", report.basic_stats.delete_operations);
        println!("  \"cache_hit_ratio\": {:.4},", report.computed_metrics.cache_hit_ratio);
        println!("  \"average_parse_time_ns\": {:.2},", report.computed_metrics.average_parse_time_ns);
        println!("  \"error_rate\": {:.6}", report.computed_metrics.error_rate);
        println!("}}");
        
        // 3. CSV格式表级数据
        println!("\n--- CSV格式表级数据 ---");
        println!("database,table,rows_processed,bytes_processed,avg_row_size,inserts,updates,deletes,errors");
        for table in &report.top_tables {
            println!("{},{},{},{},{:.2},{},{},{},{}",
                table.database_name,
                table.table_name,
                table.rows_processed,
                table.bytes_processed,
                table.avg_row_size,
                table.operations.inserts,
                table.operations.updates,
                table.operations.deletes,
                table.error_count
            );
        }
        
        // 4. Prometheus指标格式
        println!("\n--- Prometheus指标格式 ---");
        println!("# HELP binlog_rows_parsed_total Total number of rows parsed");
        println!("# TYPE binlog_rows_parsed_total counter");
        println!("binlog_rows_parsed_total {}", report.basic_stats.total_rows_parsed);
        println!("# HELP binlog_parse_duration_seconds Average parse duration");
        println!("# TYPE binlog_parse_duration_seconds gauge");
        println!("binlog_parse_duration_seconds {:.6}", report.computed_metrics.average_parse_time_ns / 1_000_000_000.0);
        println!("# HELP binlog_error_rate Error rate");
        println!("# TYPE binlog_error_rate gauge");
        println!("binlog_error_rate {:.6}", report.computed_metrics.error_rate);
        println!("# HELP binlog_cache_hit_ratio Cache hit ratio");
        println!("# TYPE binlog_cache_hit_ratio gauge");
        println!("binlog_cache_hit_ratio {:.4}", report.computed_metrics.cache_hit_ratio);
    }
    
    // 辅助格式化函数
    fn format_number(n: u64) -> String {
        if n >= 1_000_000_000 {
            format!("{:.2}B", n as f64 / 1_000_000_000.0)
        } else if n >= 1_000_000 {
            format!("{:.2}M", n as f64 / 1_000_000.0)
        } else if n >= 1_000 {
            format!("{:.2}K", n as f64 / 1_000.0)
        } else {
            n.to_string()
        }
    }
    
    fn format_bytes(bytes: u64) -> String {
        if bytes >= 1_073_741_824 {
            format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
        } else if bytes >= 1_048_576 {
            format!("{:.2} MB", bytes as f64 / 1_048_576.0)
        } else if bytes >= 1_024 {
            format!("{:.2} KB", bytes as f64 / 1_024.0)
        } else {
            format!("{} bytes", bytes)
        }
    }
    
    fn format_bytes_f64(bytes: f64) -> String {
        if bytes >= 1_073_741_824.0 {
            format!("{:.2} GB", bytes / 1_073_741_824.0)
        } else if bytes >= 1_048_576.0 {
            format!("{:.2} MB", bytes / 1_048_576.0)
        } else if bytes >= 1_024.0 {
            format!("{:.2} KB", bytes / 1_024.0)
        } else {
            format!("{:.2} bytes", bytes)
        }
    }
    
    fn format_duration_ns(ns: u64) -> String {
        if ns >= 1_000_000_000 {
            format!("{:.2}s", ns as f64 / 1_000_000_000.0)
        } else if ns >= 1_000_000 {
            format!("{:.2}ms", ns as f64 / 1_000_000.0)
        } else if ns >= 1_000 {
            format!("{:.2}μs", ns as f64 / 1_000.0)
        } else {
            format!("{}ns", ns)
        }
    }
}
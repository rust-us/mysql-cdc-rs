# 行解析监控和统计使用手册

## 概述

行解析监控系统提供了全面的binlog行解析性能监控、数据复杂度分析和错误统计功能。该系统可以帮助开发者和运维人员：

- 监控解析性能和吞吐量
- 分析数据复杂度和模式
- 跟踪错误和异常情况
- 优化解析器配置
- 进行容量规划

## 快速开始

### 1. 创建带监控的解析器

```rust
use binlog::row::parser::RowParser;
use binlog::row::monitoring::MonitoringConfig;

// 使用默认监控配置
let mut parser = RowParser::with_default_cache();

// 或使用自定义配置
let config = MonitoringConfig {
    enable_complexity_analysis: true,
    enable_table_metrics: true,
    enable_realtime_metrics: true,
    sample_rate: 1.0, // 监控所有行
    max_tracked_tables: 1000,
    moving_average_window: 100,
    memory_tracking_interval_ms: 1000,
};
let mut parser = RowParser::new_with_monitoring(1000, config);
```

### 2. 正常使用解析器

```rust
// 监控会自动记录所有解析操作
let rows = parser.parse_insert_rows(&mut cursor, table_id, &columns_present)?;
let update_rows = parser.parse_update_row_data_list(&mut cursor, table_id, &before_image, &after_image)?;
let delete_rows = parser.parse_delete_rows(&mut cursor, table_id, &columns_present)?;
```

### 3. 查看统计信息

```rust
// 获取人类可读的摘要
let summary = parser.generate_monitoring_summary();
println!("{}", summary);

// 获取详细统计报告
let report = parser.get_statistics_report();
```

## API 参考

### 解析器创建

#### RowParser 构造方法

```rust
// 使用默认缓存大小(1000)创建解析器
let parser = RowParser::with_default_cache();

// 使用自定义缓存大小创建解析器
let parser = RowParser::new(cache_size);

// 使用自定义监控配置创建解析器
let config = MonitoringConfig {
    enable_complexity_analysis: true,
    enable_table_metrics: true,
    enable_realtime_metrics: true,
    sample_rate: 1.0, // 监控所有行
    max_tracked_tables: 1000,
    moving_average_window: 100,
    memory_tracking_interval_ms: 1000,
};
let parser = RowParser::new_with_monitoring(cache_size, config);

// 创建兼容模式解析器(禁用优化)
let parser = RowParser::new_legacy(cache_size);
```

### 表映射管理

#### 表映射注册和查询

```rust
// 注册表映射事件
parser.register_table_map(table_id, table_map_event)?;

// 获取表映射(不记录缓存统计)
let table_map = parser.get_table_map(table_id)?;

// 获取表映射并记录缓存统计
let table_map = parser.get_table_map_with_stats(table_id)?;

// 清空表映射缓存
parser.clear_cache()?;

// 获取缓存大小
let cache_size = parser.cache_size()?;
```

### 行数据解析方法

#### INSERT 事件解析

```rust
// 解析INSERT行数据(带事件处理器通知)
let rows = parser.parse_insert_rows(
    &mut cursor, 
    table_id, 
    &columns_present
)?;

// 通用行数据解析(兼容旧版本)
let rows = parser.parse_row_data_list(
    &mut cursor, 
    table_id, 
    &columns_present
)?;
```

#### UPDATE 事件解析

```rust
// 基础UPDATE解析
let update_rows = parser.parse_update_row_data_list(
    &mut cursor,
    table_id,
    &before_image,
    &after_image
)?;

// 增强UPDATE解析(支持差异检测和部分列)
let update_rows = parser.parse_update_row_data_list_enhanced(
    &mut cursor,
    table_id,
    &before_image,
    &after_image,
    true, // 启用差异检测
    Some(&[0, 1, 2]) // 只处理指定列
)?;

// 增量UPDATE解析(内存优化)
let incremental_updates = parser.parse_incremental_update_data(
    &mut cursor,
    table_id,
    &before_image,
    &after_image,
    true // 只返回有变化的列
)?;
```

#### DELETE 事件解析

```rust
// 解析DELETE行数据(带事件处理器通知)
let rows = parser.parse_delete_rows(
    &mut cursor, 
    table_id, 
    &columns_present
)?;
```

### 监控配置

#### MonitoringConfig 结构

```rust
pub struct MonitoringConfig {
    /// 启用复杂度分析
    pub enable_complexity_analysis: bool,
    /// 启用表级别指标
    pub enable_table_metrics: bool,
    /// 启用实时指标
    pub enable_realtime_metrics: bool,
    /// 采样率 (0.0-1.0)
    pub sample_rate: f64,
    /// 最大跟踪表数量
    pub max_tracked_tables: usize,
    /// 移动平均窗口大小
    pub moving_average_window: usize,
    /// 内存跟踪间隔(毫秒)
    pub memory_tracking_interval_ms: u64,
}
```

**配置示例:**
```rust
// 生产环境配置
let production_config = MonitoringConfig {
    enable_complexity_analysis: true,
    enable_table_metrics: true,
    enable_realtime_metrics: true,
    sample_rate: 0.1, // 10%采样率
    max_tracked_tables: 500,
    moving_average_window: 50,
    memory_tracking_interval_ms: 5000,
};

// 开发环境配置
let development_config = MonitoringConfig {
    enable_complexity_analysis: true,
    enable_table_metrics: true,
    enable_realtime_metrics: true,
    sample_rate: 1.0, // 100%采样率
    max_tracked_tables: 100,
    moving_average_window: 20,
    memory_tracking_interval_ms: 1000,
};

// 高性能配置(最小监控开销)
let high_performance_config = MonitoringConfig {
    enable_complexity_analysis: false,
    enable_table_metrics: false,
    enable_realtime_metrics: true,
    sample_rate: 0.01, // 1%采样率
    max_tracked_tables: 50,
    moving_average_window: 10,
    memory_tracking_interval_ms: 10000,
};
```

### 监控访问方法

#### 获取监控数据

```rust
// 获取监控系统引用
let monitor = parser.get_monitor();
let monitor_mut = parser.get_monitor_mut();

// 获取完整统计报告
let report = parser.get_statistics_report();

// 生成人类可读摘要
let summary = parser.generate_monitoring_summary();

// 重置统计数据
parser.reset_monitoring_statistics();

// 动态更新监控配置
parser.update_monitoring_config(new_config);
```

### 性能优化控制

#### 优化开关

```rust
// 启用/禁用性能优化
parser.set_optimizations_enabled(true);  // 启用优化
parser.set_optimizations_enabled(false); // 禁用优化(兼容模式)

// 清空行数据池
parser.clear_row_pool();
```

### 事件处理器

#### 事件处理器注册

```rust
// 获取事件处理器注册表
let handlers = parser.event_handlers();
let handlers_mut = parser.event_handlers_mut();

// 注册自定义处理器
// (具体实现取决于 RowEventHandlerRegistry 的API)
```

### 统计信息

#### 获取解析统计

```rust
// 获取基础解析统计
let stats = parser.get_stats();
println!("解析行数: {}", stats.rows_parsed);
println!("总字节数: {}", stats.total_bytes);
println!("平均解析时间: {:.2}μs", stats.avg_parse_time_ns / 1000.0);

// 重置统计数据
parser.reset_stats();
```

### 统计数据结构

#### StatisticsReport

```rust
pub struct StatisticsReport {
    pub basic_stats: BasicParsingStats,
    pub complexity_stats: RowComplexityStats,
    pub error_stats: ErrorStats,
    pub realtime_metrics: RealtimeMetrics,
    pub computed_metrics: ComputedMetrics,
    pub top_tables: Vec<TableMetrics>,
    pub monitoring_duration: Duration,
}
```

#### BasicParsingStats

```rust
pub struct BasicParsingStats {
    pub total_rows_parsed: u64,
    pub total_bytes_processed: u64,
    pub total_parse_time_ns: u64,
    pub insert_operations: u64,
    pub update_operations: u64,
    pub delete_operations: u64,
    pub memory_allocations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}
```

#### RowComplexityStats

```rust
pub struct RowComplexityStats {
    pub avg_row_size_bytes: f64,
    pub min_row_size_bytes: u64,
    pub max_row_size_bytes: u64,
    pub avg_columns_per_row: f64,
    pub max_columns_per_row: usize,
    pub row_size_distribution: HashMap<String, u64>,
    pub column_type_distribution: HashMap<String, u64>,
    pub null_value_stats: NullValueStats,
    pub lob_stats: LobStats,
    pub update_complexity: UpdateComplexityStats,
}
```

#### ErrorStats

```rust
pub struct ErrorStats {
    pub total_errors: u64,
    pub errors_by_type: HashMap<String, u64>,
    pub errors_by_table: HashMap<String, u64>,
    pub parse_errors: u64,
    pub io_errors: u64,
    pub memory_errors: u64,
    pub timeout_errors: u64,
    pub recoverable_errors: u64,
    pub fatal_errors: u64,
    pub error_recovery_stats: ErrorRecoveryStats,
}
```

## 使用场景

### 1. 性能监控

```rust
// 定期检查性能指标
let report = parser.get_statistics_report();

println!("吞吐量: {:.2} rows/sec", report.computed_metrics.overall_throughput_rows_per_second);
println!("平均解析时间: {:.2}μs", report.computed_metrics.average_parse_time_ns / 1000.0);
println!("缓存命中率: {:.2}%", report.computed_metrics.cache_hit_ratio * 100.0);
```

### 2. 数据分析

```rust
let complexity = &report.complexity_stats;

println!("平均行大小: {:.2} bytes", complexity.avg_row_size_bytes);
println!("最大行大小: {} bytes", complexity.max_row_size_bytes);
println!("空值百分比: {:.2}%", complexity.null_value_stats.null_percentage);

// 分析行大小分布
for (range, count) in &complexity.row_size_distribution {
    println!("大小范围 {}: {} 行", range, count);
}

// 分析列类型分布
for (col_type, count) in &complexity.column_type_distribution {
    println!("列类型 {}: {} 个", col_type, count);
}
```

### 3. 错误监控

```rust
let errors = &report.error_stats;

if errors.total_errors > 0 {
    println!("总错误数: {}", errors.total_errors);
    println!("错误率: {:.4}%", report.computed_metrics.error_rate * 100.0);
    println!("恢复成功率: {:.2}%", report.computed_metrics.recovery_success_rate * 100.0);
    
    // 按类型分析错误
    for (error_type, count) in &errors.errors_by_type {
        println!("错误类型 {}: {} 次", error_type, count);
    }
    
    // 按表分析错误
    for (table, count) in &errors.errors_by_table {
        println!("表 {} 错误: {} 次", table, count);
    }
}
```

### 4. 表级别分析

```rust
// 分析最活跃的表
for (i, table) in report.top_tables.iter().take(10).enumerate() {
    println!("{}. 表 {}.{}: {} 行, 平均 {:.2} bytes/行", 
        i + 1, 
        table.database_name, 
        table.table_name,
        table.rows_processed,
        table.avg_row_size
    );
    
    println!("   操作分布: {} INSERT, {} UPDATE, {} DELETE",
        table.operations.inserts,
        table.operations.updates,
        table.operations.deletes
    );
}
```

### 5. 实时监控

```rust
use std::time::Duration;
use std::thread;

// 实时监控循环
loop {
    let monitor = parser.get_monitor();
    let realtime = &monitor.realtime_metrics;
    
    println!("实时吞吐量: {:.2} rows/sec, {:.2} KB/sec", 
        realtime.current_rows_per_second,
        realtime.current_bytes_per_second / 1024.0);
    
    println!("移动平均解析时间: {:.2}μs", 
        realtime.moving_avg_parse_time_ns / 1000.0);
    
    if let Some(last_update) = realtime.last_update {
        println!("最后更新: {:?}秒前", 
            std::time::Instant::now().duration_since(last_update).as_secs());
    }
    
    thread::sleep(Duration::from_secs(5));
}
```

### 6. 告警和阈值检查

```rust
fn check_alerts(parser: &RowParser) {
    let report = parser.get_statistics_report();
    
    // 错误率告警
    if report.computed_metrics.error_rate > 0.01 { // 1%
        eprintln!("🚨 警告: 错误率过高 {:.2}%", 
            report.computed_metrics.error_rate * 100.0);
    }
    
    // 吞吐量告警
    if report.computed_metrics.overall_throughput_rows_per_second < 100.0 {
        eprintln!("🚨 警告: 吞吐量过低 {:.2} rows/sec", 
            report.computed_metrics.overall_throughput_rows_per_second);
    }
    
    // 缓存命中率告警
    if report.computed_metrics.cache_hit_ratio < 0.8 {
        eprintln!("🚨 警告: 缓存命中率过低 {:.2}%", 
            report.computed_metrics.cache_hit_ratio * 100.0);
    }
    
    // 内存使用告警
    if report.realtime_metrics.current_memory_usage_bytes > 1_000_000_000 { // 1GB
        eprintln!("🚨 警告: 内存使用过高 {:.2} MB", 
            report.realtime_metrics.current_memory_usage_bytes as f64 / 1_048_576.0);
    }
}
```

### 7. 配置优化建议

```rust
fn suggest_optimizations(parser: &RowParser) {
    let report = parser.get_statistics_report();
    
    // 基于复杂度统计的建议
    if report.complexity_stats.avg_row_size_bytes > 1000.0 {
        println!("💡 建议: 行数据较大({:.2} bytes)，考虑增加缓存大小", 
            report.complexity_stats.avg_row_size_bytes);
    }
    
    if report.complexity_stats.null_value_stats.null_percentage > 50.0 {
        println!("💡 建议: 空值比例较高({:.2}%)，考虑优化数据存储", 
            report.complexity_stats.null_value_stats.null_percentage);
    }
    
    // 基于性能的建议
    if report.computed_metrics.cache_hit_ratio < 0.7 {
        println!("💡 建议: 缓存命中率较低({:.2}%)，考虑增加缓存大小或调整缓存策略", 
            report.computed_metrics.cache_hit_ratio * 100.0);
    }
    
    // 基于错误率的建议
    if report.computed_metrics.error_rate > 0.005 {
        println!("💡 建议: 错误率较高({:.4}%)，检查数据质量或解析逻辑", 
            report.computed_metrics.error_rate * 100.0);
    }
}
```

### 8. 数据导出

```rust
use std::fs;
use serde_json;

// 导出为JSON格式
fn export_statistics_json(parser: &RowParser, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let report = parser.get_statistics_report();
    let json = serde_json::to_string_pretty(&report)?;
    fs::write(filename, json)?;
    Ok(())
}

// 导出为CSV格式
fn export_table_metrics_csv(parser: &RowParser, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    
    let report = parser.get_statistics_report();
    let mut file = fs::File::create(filename)?;
    
    writeln!(file, "database,table,rows_processed,bytes_processed,avg_row_size,inserts,updates,deletes,errors")?;
    
    for table in &report.top_tables {
        writeln!(file, "{},{},{},{},{:.2},{},{},{},{}",
            table.database_name,
            table.table_name,
            table.rows_processed,
            table.bytes_processed,
            table.avg_row_size,
            table.operations.inserts,
            table.operations.updates,
            table.operations.deletes,
            table.error_count
        )?;
    }
    
    Ok(())
}
```

## 性能考虑

### 1. 采样率配置

对于高吞吐量场景，可以降低采样率以减少监控开销：

```rust
let config = MonitoringConfig {
    sample_rate: 0.01, // 只监控1%的行
    enable_complexity_analysis: false, // 关闭复杂度分析
    ..Default::default()
};
```

### 2. 选择性启用功能

```rust
let config = MonitoringConfig {
    enable_complexity_analysis: false, // 关闭复杂度分析以提高性能
    enable_table_metrics: true,        // 保留表级别指标
    enable_realtime_metrics: true,     // 保留实时指标
    max_tracked_tables: 100,           // 限制跟踪的表数量
    ..Default::default()
};
```

### 3. 定期重置统计

```rust
use std::time::{Duration, Instant};

let mut last_reset = Instant::now();
let reset_interval = Duration::from_secs(3600); // 每小时重置一次

// 在主循环中
if last_reset.elapsed() > reset_interval {
    // 保存当前统计数据
    let summary = parser.generate_monitoring_summary();
    log::info!("Hourly statistics: {}", summary);
    
    // 重置统计数据
    parser.reset_monitoring_statistics();
    last_reset = Instant::now();
}
```

## 故障排除

### 1. 监控数据异常

如果监控数据显示异常，检查以下方面：

```rust
let report = parser.get_statistics_report();

// 检查采样率是否过低
if report.basic_stats.total_rows_parsed == 0 {
    println!("警告: 没有监控到任何行，检查采样率配置");
}

// 检查错误率是否过高
if report.computed_metrics.error_rate > 0.1 {
    println!("错误率过高，详细错误信息:");
    for (error_type, count) in &report.error_stats.errors_by_type {
        println!("  {}: {} 次", error_type, count);
    }
}
```

### 2. 性能问题诊断

```rust
// 检查解析性能
if report.computed_metrics.average_parse_time_ns > 100_000 { // 100μs
    println!("解析时间过长，可能的原因:");
    println!("- 行数据过大: 平均 {:.2} bytes", report.complexity_stats.avg_row_size_bytes);
    println!("- 复杂度过高: 平均 {:.2} 列", report.complexity_stats.avg_columns_per_row);
    println!("- 缓存命中率低: {:.2}%", report.computed_metrics.cache_hit_ratio * 100.0);
}
```

## 最佳实践

1. **合理配置采样率**: 在生产环境中使用较低的采样率以减少性能影响
2. **定期导出数据**: 将监控数据导出到外部系统进行长期分析
3. **设置告警阈值**: 根据业务需求设置合适的告警阈值
4. **监控趋势变化**: 关注指标的趋势变化而不仅仅是绝对值
5. **结合业务指标**: 将解析监控数据与业务指标结合分析

通过这个监控系统，您可以全面了解binlog解析的性能特征，及时发现和解决问题，优化系统配置，确保稳定高效的数据处理。
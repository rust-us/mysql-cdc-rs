# MySQL CDC for Rust (mysql-cdc-rs)

[English](./README.md)

高性能的 MySQL binlog 复制客户端，基于 Rust 实现。这是一个干净、符合 Rust 习惯的 MySQL binlog 解析器实现，全面支持 MySQL 5.6、5.7 和 8.0，包括 JSON/JSONB 类型、GTID 复制和现代 MySQL 8.0 事件等高级功能。


[gitee.com/rust_us/mysql-cdc-rs](https://gitee.com/rust_us/mysql-cdc-rs)

[github.com/rust-us/mysql-cdc-rs](https://github.com/rust-us/mysql-cdc-rs)


## 核心特性

- **高性能**: 零拷贝解析，内存优化和对象池化
- **完整事件支持**: 全面支持所有主要的 MySQL binlog 事件
- **高级行处理**: 字段级变更检测和增量更新
- **线程安全架构**: 无锁设计，本地缓存支持并发访问
- **可扩展插件系统**: 自定义事件处理器和类型解码器
- **实时监控**: 内置性能指标和统计分析
- **内存管理**: 智能内存使用监控和自动清理
- **错误恢复**: 强大的错误处理和可配置的恢复策略

## 架构概览

该库采用模块化、可扩展的架构设计，专注于高性能和可维护性：

- **事件工厂**: 动态事件解析器注册和创建
- **列解析器**: 可扩展类型系统，支持自定义解码器
- **行解析器**: 面向对象设计，零拷贝位图处理
- **元数据管理器**: 线程安全的表映射和 LRU 缓存
- **内存管理器**: 对象池化和智能资源管理
- **扩展注册器**: 自定义处理器和处理器的插件系统


## 当前状态

### 已完成功能

- **核心解析器架构**: 面向对象设计，完善的错误处理
- **高级行处理**: 零拷贝解析，字段级变更检测
- **事件处理器系统**: 同步/异步事件处理，自定义处理器
- **性能优化**: 内存池化、缓存和零拷贝操作
- **监控统计**: 实时性能指标和分析
- **类型系统**: 可扩展列解析器，自定义类型解码器支持
- **MySQL 8.0 支持**: 最新事件类型，包括 TRANSACTION_PAYLOAD_EVENT

### 开发中功能

- **元数据管理**: 统一元数据系统，GTID 状态管理
- **内存管理**: 高级内存监控和自动清理
- **异步处理**: 非阻塞事件流处理
- **插件系统**: 动态扩展加载和热插拔
- **配置管理**: 全面的配置系统，支持热重载

### 当前限制

- SSL 加密支持有限
- 分片数据包（>16MB）需要额外处理
- 部分 MySQL 8.0 高级功能仍在开发中


# Development environment
为了降低执行错误的概率并改进功能特征， 我们统一了Rust工具链的版本，并切换了以下命令：

* [Windows 操作系统如何安装 Rust 开发环境](https://zhuanlan.zhihu.com/p/704426216)

```text
 rustup default nightly
```

您可以在  ` rustup toolchain list ` 中查看它。如果没有，它将自动下载。


## 系统架构

### 系统架构图
![模块依赖图设计图](./doc/architecture/mysql-cdc-rs-architecture.png)

### 核心组件

```
┌─────────────────────────────────────────────────────────────┐
│                    Binlog Parser Core                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Event       │  │ Column      │  │ Row                 │  │
│  │ Factory     │  │ Parser      │  │ Parser              │  │
│  │ 事件工厂     │  │ 列解析器     │  │ 行解析器             │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Event       │  │ Metadata    │  │ Memory              │  │
│  │ Decoder     │  │ Manager     │  │ Manager             │  │
│  │ 事件解码器   │  │ 元数据管理器 │  │ 内存管理器           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Stream      │  │ Error       │  │ Extension           │  │
│  │ Reader      │  │ Handler     │  │ Registry            │  │
│  │ 流读取器     │  │ 错误处理器   │  │ 扩展注册器           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 模块结构

```
+-- binlog/              # 核心 binlog 解析引擎
    +-- events/          # 事件类型定义和解析器
    +-- column/          # 列类型系统和解码器
    +-- row/             # 行级事件处理
    +-- decoder/         # 事件解码和注册器
    +-- metadata/        # 表元数据和 GTID 管理
    +-- factory/         # 事件工厂和创建
+-- binlog_cli/          # 命令行界面
+-- common/              # 共享类型和工具
+-- connection/          # MySQL 连接和复制
+-- memory/              # 内存管理和分配
+-- relay_log/           # 中继日志处理
+-- web/                 # Web 界面和监控
+-- tests/               # 全面测试套件
```


## 快速开始

### 安装

在 `Cargo.toml` 中添加：

```toml
[dependencies]
mysql-cdc-rs = { git = "https://github.com/your-repo/mysql-cdc-rs" }
```

### 基本用法

```rust
use mysql_cdc_rs::binlog::BinlogParser;
use mysql_cdc_rs::binlog::ParserConfig;

// 使用默认配置创建解析器
let config = ParserConfig::default();
let mut parser = BinlogParser::new(config)?;

// 解析 binlog 文件
let events = parser.parse_file("mysql-bin.000001")?;
for event in events {
    match event {
        BinlogEvent::WriteRows(write_event) => {
            println!("INSERT: {} 行", write_event.rows.len());
        }
        BinlogEvent::UpdateRows(update_event) => {
            println!("UPDATE: {} 行", update_event.rows.len());
        }
        BinlogEvent::DeleteRows(delete_event) => {
            println!("DELETE: {} 行", delete_event.rows.len());
        }
        _ => {}
    }
}
```

### 使用自定义处理器的高级用法

```rust
use mysql_cdc_rs::binlog::row::RowEventHandler;

struct MyRowHandler;

impl RowEventHandler for MyRowHandler {
    fn on_row_insert(&self, table: &TableMapEvent, row: &RowData) -> Result<()> {
        println!("在 {}.{} 中插入新行", table.database_name, table.table_name);
        Ok(())
    }
    
    fn on_row_update(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<()> {
        println!("在 {}.{} 中更新行", table.database_name, table.table_name);
        Ok(())
    }
}

// 注册自定义处理器
let mut parser = BinlogParser::new(config)?;
parser.event_handlers_mut().register_sync_handler(Arc::new(MyRowHandler));
```

### 从源码构建

```bash
# 克隆仓库
git clone https://github.com/your-repo/mysql-cdc-rs.git
cd mysql-cdc-rs

# 优化构建
cargo build --release

# 运行测试
cargo test

# 构建 CLI 工具
cargo build --bin binlog_cli --release
```


## FAQ
### Windows环境编译报错。 error: failed to run custom build command for `openssl-sys v0.9.102`
```
Caused by:
  process didn't exit successfully: `mysql-cdc-rs\target\debug\build\openssl-sys-94071a3d762a0669\build-script-main` (exit code: 101)
  --- stdout
  cargo:rerun-if-env-changed=X86_64_PC_WINDOWS_MSVC_OPENSSL_NO_VENDOR
  X86_64_PC_WINDOWS_MSVC_OPENSSL_NO_VENDOR unset
  cargo:rerun-if-env-changed=OPENSSL_NO_VENDOR
  OPENSSL_NO_VENDOR unset
  running "perl" "./Configure" "--prefix=/mysql-cdc-rs/target/debug/build/openssl-sys-5ad9f46fc53da764/out/openssl-build/install" "--openssldir=SYS$MANAGER:[OPENSSL]" "no-dso" "no-shared" "no-ssl3" "no-tests" "no-comp" "no-zlib" "no-zlib-dynamic" "--libdir=lib" "no-md2" "no-rc5" "no-weak-ssl-ciphers" "no-camellia" "no-idea" "no-seed" "no-capieng" "no-asm" "VC-WIN64A"

  Error configuring OpenSSL build:
      Command: "perl" "./Configure" "--prefix=/mysql-cdc-rs/target/debug/build/openssl-sys-5ad9f46fc53da764/out/openssl-build/install" "--openssldir=SYS$MANAGER:[OPENSSL]" "no-dso" "no-shared" "no-ssl3" "no-tests" "no-comp" "no-zlib" "no-zlib-dynamic" "--libdir=lib" "no-md2" "no-rc5" "no-weak-ssl-ciphers" "no-camellia" "no-idea" "no-seed" "no-capieng" "no-asm" "VC-WIN64A"
      Failed to execute: program not found

```
说明使用perl程序进行编译配置。
从 https://strawberryperl.com/ 下载windows安装包。
安装后，重启命令行终端， 再次build。

[strawberry-perl-5.38.2.2-64bit.msi](https://objects.githubusercontent.com/github-production-release-asset-2e65be/23202375/9607de7a-4b03-487e-ba56-8ab08fbc6f1b?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=releaseassetproduction%2F20240719%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240719T060555Z&X-Amz-Expires=300&X-Amz-Signature=6a4ea30401540fca31ca2e92d6fee29883633484d50b226bddd0e85dc728eb6a&X-Amz-SignedHeaders=host&actor_id=9307298&key_id=0&repo_id=23202375&response-content-disposition=attachment%3B%20filename%3Dstrawberry-perl-5.38.2.2-64bit.msi&response-content-type=application%2Foctet-stream)



# CLI
See [BinlogCLI README.md](binlog_cli/README.md)

## 演示
http://s.codealy.com/rust_us/mysql_cdc_rs/2024.02%20binlog%20cli%20view.webm


## 支持的事件

详细的事件文档请参见 [Binlog README.md](binlog/README.md)。

这是一个纯 Rust 实现，不需要 MySQL 服务器库，同时支持文件解析和从 MySQL 主服务器进行实时复制。目标是以最高的准确性和性能解析 binlog 事件的每个字段。

### 事件支持矩阵

| Hex  | Event Name                | Support             | Tested           | Noted |
|------|---------------------------|---------------------|------------------|-------|
| 0x00 | UNKNOWN_EVENT             | support and         | not tested       |       |
| 0x01 | START_EVENT_V3            | too old and support | not tested       |       |
| 0x02 | QUERY_EVENT               | support             | tested           |       |
| 0x03 | STOP_EVENT                | support             | not tested       |       |
| 0x04 | ROTATE_EVENT              | support             | tested           |       |
| 0x05 | INTVAR_EVENT              | support             | tested           |       |
| 0x06 | LOAD_EVENT                | not fully support   | not tested       |       |
| 0x07 | SLAVE_EVENT               | not fully support   | not tested       |       |
| 0x08 | CREATE_FILE_EVENT         | not fully support   | not tested       |       |
| 0x09 | APPEND_BLOCK_EVENT        | not fully support   | not tested       |       |
| 0x0a | EXEC_LOAD_EVENT           | not fully support   | not tested       |       |
| 0x0b | DELETE_FILE_EVENT         | not fully support   | not tested       |       |
| 0x0c | NEW_LOAD_EVENT            | support             | not tested       |       |
| 0x0d | RAND_EVENT                | support             | not tested       |       |
| 0x0e | USER_VAR_EVENT            | support             | not fully tested |       |
| 0x0f | FORMAT_DESCRIPTION_EVENT  | support             | tested           |       |
| 0x10 | XID_EVENT                 | not fully support   | tested           |       |
| 0x11 | BEGIN_LOAD_QUERY_EVENT    | not fully support   | tested           |       |
| 0x12 | EXECUTE_LOAD_QUERY_EVENT  | not fully support   | not tested       |       |
| 0x13 | TABLE_MAP_EVENT           | support             | tested           |       |
| 0x14 | PreGaWriteRowsEvent(v0)   | not support         | not tested       |       |
| 0x15 | PreGaUpdateRowsEvent(v0)  | not support         | not tested       |       |
| 0x16 | PreGaDeleteRowsEvent(v0)  | not support         | not tested       |       |
| 0x17 | WRITE_ROWS_EVENTv1        | support             | tested           |       |
| 0x18 | UPDATE_ROWS_EVENTv1       | support             | tested           |       |
| 0x19 | DELETE_ROWS_EVENTv1       | support             | tested           |       |
| 0x1a | INCIDENT_EVENT            | not fully support   | not tested       |       |
| 0x1b | HEARTBEAT_EVENT           | not fully support   | not tested       |       |
| 0x1c | IGNORABLE_EVENT           | support             | not tested       |       |
| 0x1d | ROWS_QUERY_EVENT          | not fully support   | not fully tested |       |
| 0x1e | WRITE_ROWS_EVENTv2        | support             | not fully tested |       |
| 0x1f | UPDATE_ROWS_EVENTv2       | support             | not fully tested |       |
| 0x20 | DELETE_ROWS_EVENTv2       | support             | not fully tested |       |
| 0x21 | GTID_EVENT                | support             | tested           |       |
| 0x22 | ANONYMOUS_GTID_EVENT      | support             | tested           |       |
| 0x23 | PREVIOUS_GTIDS_EVENT      | support             | tested           |       |
| 0x24 | TRANSACTION_CONTEXT_EVENT | not support         | not tested       |       |
| 0x25 | VIEW_CHANGE_EVENT         | not support         | not tested       |       |
| 0x26 | XA_PREPARE_LOG_EVENT      | not support         | not tested       |       |
| 0x27 | PARTIAL_UPDATE_ROWS_EVENT | support             | tested           | 新增 |
| 0x28 | TRANSACTION_PAYLOAD_EVENT | support             | tested           | 新增 |
| 0x29 | HEARTBEAT_LOG_EVENT_V2    | support             | tested           | 新增 |

### 最新改进

- **增强行解析器**: 零拷贝解析，字段级变更检测
- **高级监控**: 实时性能指标和统计分析
- **内存优化**: 对象池化和智能资源管理
- **事件处理器系统**: 可扩展的同步/异步事件处理
- **MySQL 8.0 支持**: 完整支持最新事件类型
- **错误恢复**: 强大的错误处理和可配置策略

## 性能特性

- **零拷贝解析**: 最小化内存分配和数据复制
- **对象池化**: 重用事件对象以减少 GC 压力
- **并发处理**: 线程安全设计，本地缓存
- **内存监控**: 实时内存使用跟踪和限制
- **增量更新**: 字段级变更检测，高效处理
- **流式支持**: 以恒定内存使用处理大型 binlog 文件

## 扩展系统

该库提供全面的插件系统用于定制：

- **事件过滤器**: 基于自定义条件过滤事件
- **事件处理器**: 使用自定义业务逻辑处理事件
- **类型解码器**: 添加对自定义 MySQL 数据类型的支持
- **行处理器**: 使用自定义逻辑处理行级变更
- **监控扩展**: 添加自定义指标和监控

## 监控统计

内置监控提供详细洞察：

- **解析性能**: 行/秒、字节/秒、解析时间
- **内存使用**: 当前使用量、峰值使用量、分配模式
- **错误统计**: 错误率、恢复成功率、错误类型
- **缓存性能**: 命中率、缓存大小、驱逐率
- **行复杂度**: 列数、数据大小、空值百分比

## 发展路线图

### 第一阶段：核心基础设施 (已完成)
- [x] 面向对象解析器架构
- [x] 零拷贝解析优化
- [x] 事件处理器系统
- [x] 性能监控
- [x] MySQL 8.0 事件支持

### 第二阶段：高级功能 (进行中)
- [ ] 统一元数据管理系统
- [ ] 高级内存管理
- [ ] Async/await 支持
- [ ] 支持热插拔的插件系统
- [ ] 配置管理

### 第三阶段：企业级功能 (计划中)
- [ ] 分布式解析
- [ ] 高级错误恢复
- [ ] 性能分析
- [ ] 多源复制
- [ ] 云原生部署

## 贡献

我们欢迎贡献！详情请参见我们的[贡献指南](CONTRIBUTING.md)。

### 开发环境设置

1. 安装 Rust nightly 工具链
2. 克隆仓库
3. 运行测试：`cargo test`
4. 检查格式：`cargo fmt`
5. 运行 lint：`cargo clippy`

## 许可证

本项目采用 MIT 许可证 - 详情请参见 [LICENSE](LICENSE) 文件。

## 致谢

- MySQL 团队提供的优秀 binlog 格式文档
- Rust 社区提供的出色生态系统
- 帮助改进此项目的贡献者们


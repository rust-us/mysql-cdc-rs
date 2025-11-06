# MySQL CDC for Rust (mysql-cdc-rs)

[中文版](./README_ZH.md)

A high-performance MySQL binlog replication client for Rust. This is a clean, idiomatic Rust implementation of a MySQL binlog parser with comprehensive support for MySQL 5.6, 5.7, and 8.0, including advanced features like JSON/JSONB types, GTID replication, and modern MySQL 8.0 events.


[gitee.com/rust_us/mysql-cdc-rs](https://gitee.com/rust_us/mysql-cdc-rs)

[github.com/rust-us/mysql-cdc-rs](https://github.com/rust-us/mysql-cdc-rs)


## Key Features

- **High Performance**: Zero-copy parsing with memory optimization and object pooling
- **Complete Event Support**: Comprehensive support for all major MySQL binlog events
- **Advanced Row Processing**: Field-level change detection with incremental updates
- **Thread-Safe Architecture**: Lock-free design with local caching for concurrent access
- **Extensible Plugin System**: Custom event handlers and type decoders
- **Real-time Monitoring**: Built-in performance metrics and statistics
- **Memory Management**: Intelligent memory usage monitoring and automatic cleanup
- **Error Recovery**: Robust error handling with configurable recovery strategies

## Architecture Overview

The library is built with a modular, extensible architecture designed for high performance and maintainability:

- **Event Factory**: Dynamic event parser registration and creation
- **Column Parser**: Extensible type system with custom decoder support  
- **Row Parser**: Object-oriented design with zero-copy bitmap processing
- **Metadata Manager**: Thread-safe table mapping with LRU caching
- **Memory Manager**: Object pooling and intelligent resource management
- **Extension Registry**: Plugin system for custom handlers and processors


## Current Status

### Completed Features

- **Core Parser Architecture**: Object-oriented design with comprehensive error handling
- **Advanced Row Processing**: Zero-copy parsing with field-level change detection  
- **Event Handler System**: Sync/async event processing with custom handlers
- **Performance Optimization**: Memory pooling, caching, and zero-copy operations
- **Monitoring & Statistics**: Real-time performance metrics and analysis
- **Type System**: Extensible column parser with custom type decoder support
- **MySQL 8.0 Support**: Latest event types including TRANSACTION_PAYLOAD_EVENT

### In Development

- **Metadata Management**: Unified metadata system with GTID state management
- **Memory Management**: Advanced memory monitoring and automatic cleanup
- **Async Processing**: Non-blocking event stream processing
- **Plugin System**: Dynamic extension loading and hot-swapping
- **Configuration Management**: Comprehensive config system with hot-reload

### Current Limitations

- SSL encryption support is limited
- Split packets (>16MB) require additional handling
- Some advanced MySQL 8.0 features are still in development


## Architecture

### System Architecture
![Module dependency](./doc/architecture/mysql-cdc-rs-architecture.png)

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Binlog Parser Core                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Event       │  │ Column      │  │ Row                 │  │
│  │ Factory     │  │ Parser      │  │ Parser              │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Event       │  │ Metadata    │  │ Memory              │  │
│  │ Decoder     │  │ Manager     │  │ Manager             │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Stream      │  │ Error       │  │ Extension           │  │
│  │ Reader      │  │ Handler     │  │ Registry            │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Module Structure

```
+-- binlog/              # Core binlog parsing engine
    +-- events/          # Event type definitions and parsers
    +-- column/          # Column type system and decoders
    +-- row/             # Row-level event processing
    +-- decoder/         # Event decoding and registry
    +-- metadata/        # Table metadata and GTID management
    +-- factory/         # Event factory and creation
+-- binlog_cli/          # Command-line interface
+-- common/              # Shared types and utilities
+-- connection/          # MySQL connection and replication
+-- memory/              # Memory management and allocation
+-- relay_log/           # Relay log processing
+-- web/                 # Web interface and monitoring
+-- tests/               # Comprehensive test suite
```


# Development environment
In order to reduce the probability of execution errors and improve the functional features,
we unify the versions of the Rust toolchain and switch the following commands:

* [How to install Rust development environment in Windows operating system](https://zhuanlan.zhihu.com/p/704426216)

## Env

```
$ rustup install nightly

$ rustup toolchain list
stable-aarch64-apple-darwin (default)
nightly-aarch64-apple-darwin (override)

#$ rustup override set nightly
# Or 
$ rustup default nightly

$ rustup toolchain list
stable-x86_64-pc-windows-msvc (default)
nightly-x86_64-pc-windows-msvc (active)
```

You can check it out in the ` rustup toolchain list `. If not, it will be automatically downloaded.

## Test
### RunTest

```
$ cargo test
running 2 tests
test tests::bench_add_two ... ok
test tests::it_works ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

### 运行 benchmark
Install [gnuplots](http://www.gnuplot.info/)

Add
```
[dev-dependencies]
criterion = "0.3"

[[bench]]
name = "my_benchmark"
harness = false
```

编写测试文件($PROJECT/benches/my_benchmark.rs)
```
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n-1) + fibonacci(n-2),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
```

```
$ cargo bench
test result: ok. 0 passed; 0 failed; 7 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running benches\aes_bencher.rs (target\release\deps\aes_bencher-f9a9ea35aca2ab2d.exe)
Gnuplot not found, using plotters backend
add_two                 time:   [604.03 ps 619.40 ps 636.72 ps]
                        change: [-2.3923% +0.4866% +3.6080%] (p = 0.76 > 0.05)
                        No change in performance detected.

# 显示详细日志
cargo bench --bench aes_benchmark -- --verbose

# 只运行特定测试
cargo bench -p cryptolib --bench sm4_benchmark
cargo bench --bench aes_benchmark -- -n "AES Parallel"
```

报告在目录 `target\criterion` 下查看

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
mysql-cdc-rs = { git = "https://github.com/your-repo/mysql-cdc-rs" }
```

### Basic Usage

```rust
use mysql_cdc_rs::binlog::BinlogParser;
use mysql_cdc_rs::binlog::ParserConfig;

// Create parser with default configuration
let config = ParserConfig::default();
let mut parser = BinlogParser::new(config)?;

// Parse binlog file
let events = parser.parse_file("mysql-bin.000001")?;
for event in events {
    match event {
        BinlogEvent::WriteRows(write_event) => {
            println!("INSERT: {} rows", write_event.rows.len());
        }
        BinlogEvent::UpdateRows(update_event) => {
            println!("UPDATE: {} rows", update_event.rows.len());
        }
        BinlogEvent::DeleteRows(delete_event) => {
            println!("DELETE: {} rows", delete_event.rows.len());
        }
        _ => {}
    }
}
```

### Advanced Usage with Custom Handlers

```rust
use mysql_cdc_rs::binlog::row::RowEventHandler;

struct MyRowHandler;

impl RowEventHandler for MyRowHandler {
    fn on_row_insert(&self, table: &TableMapEvent, row: &RowData) -> Result<()> {
        println!("New row inserted in {}.{}", table.database_name, table.table_name);
        Ok(())
    }
    
    fn on_row_update(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<()> {
        println!("Row updated in {}.{}", table.database_name, table.table_name);
        Ok(())
    }
}

// Register custom handler
let mut parser = BinlogParser::new(config)?;
parser.event_handlers_mut().register_sync_handler(Arc::new(MyRowHandler));
```

### Build from Source

```bash
# Clone repository
git clone https://github.com/your-repo/mysql-cdc-rs.git
cd mysql-cdc-rs

# Build with optimizations
cargo build --release

# Run tests
cargo test

# Build CLI tool
cargo build --bin binlog_cli --release
```


## FAQ
### Windows environment compilation error。 error: failed to run custom build command for `openssl-sys v0.9.102`
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
Instructions for compiling and configuring using Perl programs. 
From https://strawberryperl.com/ Download the Windows installation package. 
And installation, restart the command-line terminal and build again.

[strawberry-perl-5.38.2.2-64bit.msi](https://objects.githubusercontent.com/github-production-release-asset-2e65be/23202375/9607de7a-4b03-487e-ba56-8ab08fbc6f1b?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=releaseassetproduction%2F20240719%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240719T060555Z&X-Amz-Expires=300&X-Amz-Signature=6a4ea30401540fca31ca2e92d6fee29883633484d50b226bddd0e85dc728eb6a&X-Amz-SignedHeaders=host&actor_id=9307298&key_id=0&repo_id=23202375&response-content-disposition=attachment%3B%20filename%3Dstrawberry-perl-5.38.2.2-64bit.msi&response-content-type=application%2Foctet-stream)

# CLI
See [BinlogCLI README.md](binlog_cli/README.md)

## DEMO VIEW
http://s.codealy.com/rust_us/mysql_cdc_rs/2024.02%20binlog%20cli%20view.webm


## Supported Events

See [Binlog README.md](binlog/README.md) for detailed event documentation.

This is a pure Rust implementation that doesn't require MySQL server libraries while supporting both file parsing and live replication from MySQL master servers. The goal is to parse every field of binlog events with maximum accuracy and performance.

### Event Support Matrix

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
| 0x27 | PARTIAL_UPDATE_ROWS_EVENT | support             | tested           | New |
| 0x28 | TRANSACTION_PAYLOAD_EVENT | support             | tested           | New |
| 0x29 | HEARTBEAT_LOG_EVENT_V2    | support             | tested           | New |

### Recent Improvements

- **Enhanced Row Parser**: Zero-copy parsing with field-level change detection
- **Advanced Monitoring**: Real-time performance metrics and statistics  
- **Memory Optimization**: Object pooling and intelligent resource management
- **Event Handler System**: Extensible sync/async event processing
- **MySQL 8.0 Support**: Complete support for latest event types
- **Error Recovery**: Robust error handling with configurable strategies

## Performance Features

- **Zero-Copy Parsing**: Minimize memory allocations and data copying
- **Object Pooling**: Reuse event objects to reduce GC pressure
- **Concurrent Processing**: Thread-safe design with local caching
- **Memory Monitoring**: Real-time memory usage tracking and limits
- **Incremental Updates**: Field-level change detection for efficient processing
- **Streaming Support**: Process large binlog files with constant memory usage

## Extension System

The library provides a comprehensive plugin system for customization:

- **Event Filters**: Filter events based on custom criteria
- **Event Processors**: Process events with custom business logic
- **Type Decoders**: Add support for custom MySQL data types
- **Row Handlers**: Handle row-level changes with custom logic
- **Monitoring Extensions**: Add custom metrics and monitoring

## Monitoring & Statistics

Built-in monitoring provides detailed insights:

- **Parse Performance**: Rows/second, bytes/second, parse times
- **Memory Usage**: Current usage, peak usage, allocation patterns
- **Error Statistics**: Error rates, recovery success, error types
- **Cache Performance**: Hit ratios, cache sizes, eviction rates
- **Row Complexity**: Column counts, data sizes, null percentages

## Roadmap

### Phase 1: Core Infrastructure (Completed)
- [x] Object-oriented parser architecture
- [x] Zero-copy parsing optimization
- [x] Event handler system
- [x] Performance monitoring
- [x] MySQL 8.0 event support

### Phase 2: Advanced Features (In Progress)
- [ ] Unified metadata management system
- [ ] Advanced memory management
- [ ] Async/await support
- [ ] Plugin system with hot-swapping
- [ ] Configuration management

### Phase 3: Enterprise Features (Planned)
- [ ] Distributed parsing
- [ ] Advanced error recovery
- [ ] Performance analytics
- [ ] Multi-source replication
- [ ] Cloud-native deployment

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

1. Install Rust nightly toolchain
2. Clone the repository
3. Run tests: `cargo test`
4. Check formatting: `cargo fmt`
5. Run lints: `cargo clippy`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- MySQL team for the excellent binlog format documentation
- Rust community for the amazing ecosystem
- Contributors who help improve this project

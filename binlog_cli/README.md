# Binlog CLI 操作手册

MySQL Binlog 解析工具，使用 Rust 实现的高性能 binlog 读取和解析工具。

## 功能特性

- 实时读取 MySQL binlog 事件
- 支持多种输出格式 (YAML/JSON)
- 支持配置文件和命令行参数
- 内存使用控制
- 调试模式支持
- 跨平台支持

## 安装和使用

### 开发环境使用 (打包前)

#### 前置要求
- Rust 1.70+ 
- MySQL 5.7+ 或 MySQL 8.0+

#### 编译和运行
```bash
# 克隆项目
git clone <repository-url>
cd <project-directory>

# 编译项目
cargo build --release

# 运行 CLI 工具
cargo run --bin binlog_cli -- [OPTIONS]

# 或者直接运行编译后的二进制文件
./target/release/binlog_cli [OPTIONS]
```

### 生产环境使用 (打包后)

#### 安装
```bash
# 方式1: 从 release 下载预编译二进制文件
wget https://github.com/your-repo/releases/download/v0.0.3/binlog_cli-linux-x86_64.tar.gz
tar -xzf binlog_cli-linux-x86_64.tar.gz
chmod +x binlog_cli
sudo mv binlog_cli /usr/local/bin/

# 方式2: 使用 cargo 安装
cargo install binlog_cli

# 方式3: 本地编译安装
cargo install --path ./binlog_cli
```

#### 验证安装
```bash
binlog_cli --version
```

## 命令行选项

### 帮助信息
```bash
# 查看帮助
binlog_cli -h
binlog_cli --help

# 输出示例:
MySQL binlog tool impl with Rust

Usage: binlog_cli [OPTIONS]

Options:
  -c, --config <FILE>           Path to loaded configuration file
  -d, --debug                   Enable debug mode
  -f, --format <FORMAT>         Output format: [yaml | json], default yaml
      --host <HOST>             MySQL host
      --port <PORT>             MySQL port [1-65535]
  -u, --username <USERNAME>     MySQL username  
  -p, --password <PASSWORD>     MySQL password
      --stop                    Shut down binlog cli
  -h, --help                    Print help
  -V, --version                 Print version
```

### 版本信息
```bash
binlog_cli -V
binlog_cli --version
# 输出: cdc-cli 0.0.3
```

## 使用方式

### 1. 使用命令行参数连接数据库

#### 基本连接
```bash
# 连接本地 MySQL
binlog_cli --host localhost --port 3306 -u root -p your_password

# 连接远程 MySQL， 读取 Binlog 信息
binlog_cli --host 192.168.1.100 --port 3306 -u root -p your_password

╔╦╗╔═╗ ╔═╗╔╦╗╦
 ║ ╠═╣ ║   ║ ║
 ╩ ╩ ╩ ╚═╝ ╩ ╩═╝ Rust MySQL Binlog CLI v0.0.3

CliClient start
BinlogServer start
BinlogSubscribe start
["RotateEvent" 1], pos 0 in binlog.000062

["FormatDescriptionEvent" 2], pos 126 in binlog.000062

["PreviousGtidsLogEvent" 3], pos 157 in binlog.000062

["AnonymousGtidLogEvent" 4], pos 234 in binlog.000062

["QueryEvent" 5], pos 344 in binlog.000062

["AnonymousGtidLogEvent" 6], pos 421 in binlog.000062

["QueryEvent" 7], pos 563 in binlog.000062

["RotateEvent" 8], pos 0 in binlog.000063

["FormatDescriptionEvent" 9], pos 126 in binlog.000063

["PreviousGtidsLogEvent" 10], pos 157 in binlog.000063

["RotateEvent" 11], pos 0 in binlog.000064

["FormatDescriptionEvent" 12], pos 126 in binlog.000064

["PreviousGtidsLogEvent" 13], pos 157 in binlog.000064

binlog 读取完成，耗时：97ms， 收包总大小 1.01 KB bytes.
load_read_ptr: [13], pos 157 in binlog.000064
Binlog CLI started successfully
```

#### 启用调试模式
```bash
# 调试模式 + YAML 输出
binlog_cli -d --host localhost --port 3306 -u root -p your_password

# 调试模式 + JSON 输出
binlog_cli -d -f json --host localhost --port 3306 -u root -p your_password
```

#### 指定输出格式
```bash
# YAML 格式输出 (默认)
binlog_cli -f yaml --host localhost --port 3306 -u root -p your_password

# JSON 格式输出
binlog_cli -f json --host localhost --port 3306 -u root -p your_password
```

### 2. 使用配置文件

#### 创建配置文件
首先创建配置文件 `conf/replayer.toml`:

```toml
[base]
log_dir = "./logs"

[binlog]
host = "localhost"
port = 3306
username = "root"
password = "your_password"
server_id = 1001
```

#### 使用配置文件运行
```bash
# 使用默认配置文件 (conf/replayer.toml)
binlog_cli

# 指定配置文件路径
binlog_cli -c /path/to/your/config.toml

# 配置文件 + 调试模式
binlog_cli -d -c conf/replayer.toml
```

### 3. 混合使用 (配置文件 + 命令行参数)

命令行参数会覆盖配置文件中的相应设置:

```bash
# 使用配置文件，但覆盖主机地址
binlog_cli -c conf/replayer.toml --host 192.168.1.200

# 使用配置文件，但覆盖端口和用户名
binlog_cli -c conf/replayer.toml --port 3307 -u admin
```

## 输出示例

### YAML 格式输出
```yaml
event_type: WriteRows
timestamp: 2024-01-15T10:30:45Z
database: test_db
table: users
data:
  - id: 1
    name: "John Doe"
    email: "john@example.com"
```

### JSON 格式输出
```json
{
  "event_type": "WriteRows",
  "timestamp": "2024-01-15T10:30:45Z",
  "database": "test_db",
  "table": "users",
  "data": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com"
    }
  ]
}
```

## 常见使用场景

### 1. 数据同步监控
```bash
# 实时监控数据变更
binlog_cli -f json --host prod-mysql --port 3306 -u replication -p password > changes.log
```

### 2. 数据审计
```bash
# 记录所有数据变更用于审计
binlog_cli -d -f yaml -c audit.toml | tee audit.log
```

### 3. 开发调试
```bash
# 开发环境调试数据变更
binlog_cli -d --host localhost --port 3306 -u root -p dev_password
```

## 故障排除

### 常见错误

1. **连接失败**
   ```bash
   Error: Failed to connect to MySQL server
   ```
   - 检查主机地址、端口、用户名和密码
   - 确保 MySQL 服务正在运行
   - 检查网络连接

2. **权限不足**
   ```bash
   Error: Access denied for user
   ```
   - 确保用户具有 REPLICATION SLAVE 权限
   - 检查用户密码是否正确

3. **Binlog 未启用**
   ```bash
   Error: Binary logging is not enabled
   ```
   - 在 MySQL 配置中启用 binlog
   - 重启 MySQL 服务

### 调试技巧

1. **启用调试模式**
   ```bash
   binlog_cli -d [other options]
   ```

2. **检查配置**
   ```bash
   # 查看加载的配置信息
   binlog_cli -d -c conf/replayer.toml
   ```

3. **查看日志**
   ```bash
   # 日志文件位置 (默认: ./logs/)
   tail -f logs/binlog_cli.log
   ```

## 性能优化

### 内存使用控制
```bash
# 限制最大内存使用 (未来版本支持)
binlog_cli --max-memory 1GB --host localhost -u root -p password
```

### 批处理模式
```bash
# 批量处理历史数据
binlog_cli -f json --host localhost -u root -p password > historical_data.json
```

## 配置文件详解

完整的配置文件示例:

```toml
[base]
log_dir = "./logs"

[binlog]
host = "localhost"
port = 3306
username = "root"
password = "your_password"
server_id = 1001
# 可选配置
# start_file = "mysql-bin.000001"
# start_position = 4
# database_filter = ["db1", "db2"]
# table_filter = ["table1", "table2"]
```

## 开发和贡献

### 本地开发
```bash
# 运行测试
cargo test

# 运行 clippy 检查
cargo clippy

# 格式化代码
cargo fmt

# 运行开发版本
cargo run --bin binlog_cli -- -d --host localhost -u root -p password
```

### 构建发布版本
```bash
# 构建优化版本
cargo build --release

# 构建所有平台版本 (需要交叉编译工具)
cargo build --release --target x86_64-unknown-linux-gnu
cargo build --release --target x86_64-pc-windows-gnu
cargo build --release --target x86_64-apple-darwin
```
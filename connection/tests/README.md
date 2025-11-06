# Connection 集成测试

本目录包含 connection 模块的集成测试，这些测试需要连接到真实的 MySQL 服务器。

## 配置方式

### 方式一：配置文件（推荐）

1. 复制配置文件模板：
   ```bash
   cp test-config.toml.example test-config.toml
   ```

2. 编辑 `test-config.toml` 文件，填入你的 MySQL 服务器信息：
   ```toml
   [mysql]
   host = "localhost"
   port = 3306
   username = "root"
   password = "your_password"
   database = "test"
   
   [test]
   enabled = true
   verbose = false
   
   [binlog]
   enabled = true
   buffer_size = 3072
   ```

3. 运行测试：
   ```bash
   cargo test --package connection --test integration_tests
   ```

### 方式二：环境变量（备用）

如果没有配置文件，测试会回退到环境变量方式：

```bash
export MYSQL_TEST_URL=mysql://localhost:3306
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=your_password
cargo test --package connection --test integration_tests
```

## 配置选项说明

### [mysql] 部分
- `host`: MySQL 服务器地址
- `port`: MySQL 服务器端口
- `username`: 数据库用户名
- `password`: 数据库密码
- `database`: 数据库名（可选，默认 "test"）
- `timeout`: 连接超时时间（可选，默认 30 秒）
- `ssl`: 是否启用 SSL（可选，默认 false）

### [test] 部分
- `enabled`: 是否启用集成测试（true/false）
- `verbose`: 是否输出详细日志（true/false）

### [binlog] 部分
- `enabled`: 是否启用 binlog 测试（需要 REPLICATION 权限）
- `buffer_size`: binlog 缓冲区大小（字节）

## MySQL 服务器要求

### 基本要求
- MySQL 5.7+ 或 MySQL 8.0+
- 用户需要有基本的 SELECT、INSERT、UPDATE、DELETE 权限

### Binlog 测试要求
如果要运行 binlog 相关测试，MySQL 用户还需要：
- `REPLICATION SLAVE` 权限
- `REPLICATION CLIENT` 权限
- MySQL 服务器需要启用 binlog

可以使用以下 SQL 授权：
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'your_user'@'%';
FLUSH PRIVILEGES;
```

## 跳过测试

如果你不想运行集成测试，可以：

1. 在配置文件中设置 `test.enabled = false`
2. 或者不创建配置文件且不设置环境变量
3. 或者运行时排除集成测试：
   ```bash
   cargo test --package connection --lib
   ```

## 故障排除

### 连接失败
- 检查 MySQL 服务器是否正在运行
- 验证主机名、端口、用户名和密码是否正确
- 确认防火墙设置允许连接

### Binlog 测试失败
- 确认用户有 REPLICATION 权限
- 检查 MySQL 是否启用了 binlog：
  ```sql
  SHOW VARIABLES LIKE 'log_bin';
  ```
- 如果不需要 binlog 测试，可以在配置中设置 `binlog.enabled = false`
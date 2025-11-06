# Binlog CLI 基本使用示例

## 1. 快速开始

### 使用命令行参数连接数据库
```bash
# 连接本地 MySQL 数据库
binlog_cli --host localhost --port 3306 -u root -p your_password

# 启用调试模式查看详细信息
binlog_cli -d --host localhost --port 3306 -u root -p your_password

# 指定输出格式为 JSON
binlog_cli -f json --host localhost --port 3306 -u root -p your_password
```

### 使用配置文件
```bash
# 使用默认配置文件 conf/replayer.toml
binlog_cli

# 指定自定义配置文件
binlog_cli -c /path/to/your/config.toml

# 配置文件 + 调试模式
binlog_cli -d -c conf/replayer.toml
```

## 2. 配置文件示例

### 基本配置 (conf/replayer.toml)
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

### 完整配置示例
```toml
[base]
log_dir = "./logs"
max_memory = "1GB"

[binlog]
# 数据库连接配置
host = "localhost"
port = 3306
username = "replication_user"
password = "secure_password"
server_id = 1001

# 可选: 指定起始位置
# start_file = "mysql-bin.000001"
# start_position = 4

# 可选: 数据库过滤
# database_filter = ["db1", "db2"]

# 可选: 表过滤  
# table_filter = ["users", "orders"]
```

## 3. 常见使用场景

### 数据同步监控
```bash
# 实时监控数据变更，输出到文件
binlog_cli -f json --host prod-mysql -u replication -p password > changes.log

# 监控特定数据库的变更
binlog_cli -d -c prod_config.toml | grep "database: myapp"
```

### 数据审计
```bash
# 记录所有数据变更用于审计
binlog_cli -f yaml -c audit.toml | tee audit_$(date +%Y%m%d).log
```

### 开发调试
```bash
# 开发环境调试，查看详细的 binlog 事件
binlog_cli -d --host dev-mysql --port 3306 -u root -p dev_password
```

### 数据恢复准备
```bash
# 从特定时间点开始读取 binlog
binlog_cli -f json -c recovery.toml > recovery_data.json
```

## 4. 输出格式示例

### YAML 格式 (默认)
```yaml
---
event_type: WriteRows
timestamp: "2024-01-15T10:30:45Z"
server_id: 1
database: "test_db"
table: "users"
columns:
  - name: "id"
    type: "INT"
    value: 1
  - name: "username"
    type: "VARCHAR"
    value: "john_doe"
  - name: "email"
    type: "VARCHAR"
    value: "john@example.com"
  - name: "created_at"
    type: "TIMESTAMP"
    value: "2024-01-15T10:30:45Z"
```

### JSON 格式
```json
{
  "event_type": "WriteRows",
  "timestamp": "2024-01-15T10:30:45Z",
  "server_id": 1,
  "database": "test_db",
  "table": "users",
  "columns": [
    {
      "name": "id",
      "type": "INT",
      "value": 1
    },
    {
      "name": "username", 
      "type": "VARCHAR",
      "value": "john_doe"
    },
    {
      "name": "email",
      "type": "VARCHAR", 
      "value": "john@example.com"
    },
    {
      "name": "created_at",
      "type": "TIMESTAMP",
      "value": "2024-01-15T10:30:45Z"
    }
  ]
}
```

## 5. 高级用法

### 结合其他工具使用
```bash
# 使用 jq 处理 JSON 输出
binlog_cli -f json -c config.toml | jq '.[] | select(.database == "myapp")'

# 使用 grep 过滤特定事件
binlog_cli -d -c config.toml | grep -E "(INSERT|UPDATE|DELETE)"

# 重定向到不同文件
binlog_cli -f json -c config.toml | tee >(grep "users" > users_changes.log) >(grep "orders" > orders_changes.log)
```

### 性能监控
```bash
# 监控 binlog 处理性能
binlog_cli -d -c config.toml 2>&1 | grep -E "(processed|memory|position)"
```

## 6. 故障排除

### 常见问题和解决方案

1. **连接被拒绝**
   ```bash
   # 检查 MySQL 服务状态
   systemctl status mysql
   
   # 检查端口是否开放
   netstat -tlnp | grep 3306
   ```

2. **权限不足**
   ```sql
   -- 为用户授予复制权限
   GRANT REPLICATION SLAVE ON *.* TO 'replication_user'@'%';
   FLUSH PRIVILEGES;
   ```

3. **Binlog 未启用**
   ```sql
   -- 检查 binlog 状态
   SHOW VARIABLES LIKE 'log_bin';
   
   -- 查看 binlog 文件
   SHOW BINARY LOGS;
   ```

4. **调试连接问题**
   ```bash
   # 使用调试模式查看详细错误信息
   binlog_cli -d --host your-host --port 3306 -u your-user -p your-password
   ```

## 7. 最佳实践

### 生产环境建议
1. 使用专门的复制用户，限制权限
2. 配置适当的内存限制
3. 定期轮转日志文件
4. 监控 binlog 处理延迟
5. 设置合适的重连机制

### 安全建议
1. 不要在命令行中直接输入密码
2. 使用配置文件存储敏感信息
3. 限制配置文件的访问权限
4. 使用 SSL 连接 (如果支持)

### 性能优化
1. 根据需要调整内存限制
2. 使用过滤器减少不必要的数据处理
3. 选择合适的输出格式
4. 定期清理日志文件
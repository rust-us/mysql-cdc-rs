# Connection Module

This module provides MySQL connection functionality for the binlog parser.

## Running Tests

### Unit Tests
Most tests are unit tests that don't require external dependencies:
```bash
cargo test --package connection --lib
```

### Integration Tests with MySQL
Integration tests require a running MySQL server and are located in `tests/integration_tests.rs`. These tests are automatically skipped if MySQL configuration is not provided.

To run integration tests:

1. **Create configuration file**:
   ```bash
   # Copy the configuration template
   cp connection/tests/test-config.toml.example connection/tests/test-config.toml
   ```

2. **Edit configuration file** (`connection/tests/test-config.toml`):
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

3. **Run integration tests**:
   ```bash
   cargo test --package connection --test integration_tests
   ```



### Docker MySQL for Testing
You can use Docker to run a MySQL instance for testing:

```bash
# Start MySQL container
docker run --name mysql-test -e MYSQL_ROOT_PASSWORD=123456 -p 3306:3306 -d mysql:8.0

# Wait for MySQL to start up
sleep 30

# Create test configuration
cp connection/tests/test-config.toml.example connection/tests/test-config.toml

# Edit the configuration file to match Docker MySQL settings:
# host = "localhost"
# port = 3306
# username = "root"
# password = "123456"
# [test] enabled = true
# [binlog] enabled = true

# Run all tests including integration tests
cargo test --package connection --lib
cargo test --package connection --test integration_tests

# Clean up
docker stop mysql-test
docker rm mysql-test
```

## Test Categories

- **Unit Tests** (`src/**/*.rs`): Test individual components without external dependencies
- **Integration Tests** (`tests/integration_tests.rs`): Test complete workflows with MySQL server
  - `test_mysql_connection`: Tests basic connection functionality
  - `test_mysql_binlog_connection`: Tests binlog connection functionality  
  - `test_mysql_binlog_events`: Tests binlog event reading (requires REPLICATION privileges)
  - `test_connection_error_handling`: Tests error handling for connection failures

## Configuration Options

The test configuration file supports the following options:

### MySQL Connection (`[mysql]`)
- `host`: MySQL server hostname (default: "localhost")
- `port`: MySQL server port (default: 3306)
- `username`: Database username (default: "root")
- `password`: Database password (default: "123456")
- `database`: Database name (default: "test")
- `timeout`: Connection timeout in seconds (default: 30)
- `ssl`: Enable SSL connection (default: false)

### Test Settings (`[test]`)
- `enabled`: Enable/disable integration tests (default: true)
- `verbose`: Enable verbose logging during tests (default: false)

### Binlog Settings (`[binlog]`)
- `enabled`: Enable/disable binlog-specific tests (default: true)
- `buffer_size`: Binlog buffer size in bytes (default: 3072)

## Requirements for Integration Tests

- MySQL server running on specified host/port
- Valid user credentials
- For binlog tests: user must have REPLICATION CLIENT and REPLICATION SLAVE privileges
- For binlog tests: MySQL server must have binlog enabled (`log-bin` configuration)
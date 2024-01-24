# binlog 解析模块

使用方式
```rust
let factory = EventFactory::new(true);

factory.parser_bytes(...);
```

# Support
是一个基于 Rust 实现的 MySQL binlog 文件解析库，
纯 Rust 实现，无需 mysql-server 库， 但同时也可以订阅 mysql-master-server。

项目的目标是尽量解析 binlog 事件的每个字段。

Parsed events matrix:

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
| 0x27 | PARTIAL_UPDATE_ROWS_EVENT | not support         | not tested       |       |
| 0x28 | TRANSACTION_PAYLOAD_EVENT | not support         | not tested       |       |
| 0x29 | HEARTBEAT_LOG_EVENT_V2    | not support         | not tested       |       |

# debug model
add `-- --nocapture`
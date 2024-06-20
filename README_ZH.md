# ForMe

[English](./README.md)

Rust CDC 客户端。 是MySQL binlog解析器的干净、方便的 Rust实现，

包括对MySQL 5.6/5.7/8.0中引入的JSONB类型的支持。

其主要目的是处理基于行的日志记录消息， 已经针对（MySQL）5.6、5.7和8.0进行了测试。

这个库试图在解析完整的binlog文件后允许定制各种不同输出端的实现，所有感兴趣的数据类型都可以使用Serde进行序列化，

因此很容易连接到其他数据处理流中。


# Limitations

请注意，目前有以下限制：
* 仅支持标准身份验证插件mysql_native_password和caching_sha2_password。
* 目前，该库不支持SSL加密。
* 不处理拆分数据包（16MB及以上）。


# Development environment
为了降低执行错误的概率并改进功能特征， 我们统一了Rust工具链的版本，并切换了以下命令：

* [Windows 操作系统如何安装 Rust 开发环境](https://zhuanlan.zhihu.com/p/704426216)

```text
 rustup default nightly
```

您可以在  ` rustup toolchain list ` 中查看它。如果没有，它将自动下载。


# Architecture
## mysql-cdc-rs-architecture
![模块依赖图设计图](./doc/architecture/mysql-cdc-rs-architecture.png)

## 模块职能设计
```

+-- binlog： binlog 事件解析的能力实现
+-- binlog-Adapter： binlog 事件数据结构转中立数据输出实现
    -- log: 默认的binlog数据的日志输出
    -- relay_log: 默认的binlog数据的中继日志输出
+-- binlog_cli： CLI 客户端
+-- common: 基本类型定义
+-- conf: 工程默认配置文件
+-- connection: 提供 MySQL/PostgreSQL/MariaDB 的连接能力和binlog订阅能力
+-- doc: 文档
+-- memory: 内存分配器
+-- raft: raft 协议(Broker Impl)
+-- relay_log: 中继日志
+-- replayer: 启动入口
+-- rpc: rpc 协议
+-- sink: 中继数据推送至Broker的服务
+-- slave: 提供mysql slave 伪装能力与dump能力
+-- tests: 测试用例

```



# How to Use


# CLI
See [BinlogCLI README.md](binlog_cli/README.md)

## 演示
http://s.codealy.com/rust_us/mysql_cdc_rs/2024.02%20binlog%20cli%20view.webm


# Support Event
See [Binlog README.md](binlog/README.md)

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


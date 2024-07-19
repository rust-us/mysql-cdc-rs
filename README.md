# ForMe

[中文版](./README_ZH.md)

MySQL binlog replication client for Rust. 
Is a clean, idomatic Rust implementation of a MySQL binlog parser, 
including support for the JSONB type introduced in MySQL 5.6/5.7/8.0.

Its primary purpose is handling row-based logging messages, 
but it has rudimentary support for older statement-based replication.
It's been tested against  (MySQL) 5.6 and 5.7 and 8.0.

This library seeks to be competitive with mysqlbinlog at time-to-parse a full binlog file. 
All interesting datatypes are serializable using Serde, 
so it's easy to hook into other data processing flows.


# Limitations

Please note the lib currently has the following limitations:
* Supports only standard auth plugins mysql_native_password and caching_sha2_password.
* Currently, the library doesn't support SSL encryption.
* Doesn't handle split packets (16MB and more).


# Development environment
In order to reduce the probability of execution errors and improve the functional features,
we unify the versions of the Rust toolchain and switch the following commands:

* [How to install Rust development environment in Windows operating system](https://zhuanlan.zhihu.com/p/704426216)

```text
 rustup default nightly
```

You can check it out in the ` rustup toolchain list `. If not, it will be automatically downloaded.


# Architecture
## mysql-cdc-rs-architecture
![Module dependency](./doc/architecture/mysql-cdc-rs-architecture.png)

## Module Design
```

+-- binlog： Implementation of the ability to parse binlog events
+-- binlog-Adapter： Implementation of converting binlog event data structure to neutral data output
    -- log: Default binlog data log output
    -- relay_log: Default relay log output for binlog data
+-- binlog_cli： CLI Client
+-- common: Basic Type Definition
+-- conf: Project default configuration file
+-- connection: Provide MySQL/PostgreSQL/MariaDB connectivity and binlog subscription capabilities
+-- doc: Documents
+-- memory: allocator
+-- raft: raft Protocol(Broker Impl)
+-- relay_log: relay logs
+-- replayer: Main
+-- rpc: rpc Protocol
+-- sink: Relay data push to broker service
+-- slave: Provide MySQL slave disguise and dump capabilities
+-- tests: test case

```


# How to Use
```
# cargo tree
cargo build


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


# Support Event
See [Binlog README.md](binlog/README.md)

It is a MySQL binlog file parsing library based on Rust implementation,

Pure Rust implementation, no need for MySQL server library, but can also subscribe to MySQL master server.

The goal of the project is to parse every field of the binlog event as much as possible.

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

# ForMe

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

```text
 rustup default nightly
```

You can check it out in the ` rustup toolchain list `. If not, it will be automatically downloaded.


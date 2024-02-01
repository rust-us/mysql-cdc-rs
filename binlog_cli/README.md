# How to Use

## help
Options:
* binlog_cli -h 
* binlog_cli -help 
* binlog_cli --help

```ssh
$ binlog_cli -help
MySQL binlog tool impl with Rust

Usage: binlog_cli [OPTIONS]

Options:
  -c, --config <FILE>            
  -d, --debug...                 enable debug mode
      --max-memory <MAX_MEMORY>  set max memory bytes to use, eg: 1GB / 200MB. default NO LIMITATION
  -h, --help                     Print help
  -V, --version                  Print version
```

## Version
```ssh
$ binlog_cli -V
cdc-cli 0.0.1
```

## enable debug
```ssh
$ binlog_cli -d
```

## 指定数据库相关配置读取 binlog
```ssh
./binlog_cli --host 192.168.42.237 --port 3306  -u root -p Aa123456

./binlog_cli -d --host 192.168.42.237 --port 3306  -u root -p Aa123456

./binlog_cli -f yaml -d --host 192.168.42.237 --port 3306 -u root -p 123456



```

## 指定配置文件读取 binlog
Options:

`binlog_cli -d --config <config_file_path>`

Example
```ssh
$ binlog_cli -d --config conf/replayer.toml 
```
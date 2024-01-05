use mysql_common::packets::{Column, OkPacket};
use mysql_common::prelude::FromRow;
use mysql_common::proto::Text;
use mysql_common::row::convert::from_row;
use mysql_common::{Row, Value};

use crate::mysql::conn::connection::ConnMut;
use crate::mysql::conn::connection::Connection;
use crate::mysql::error::Result;
use crate::mysql::handler::query::query_command::QueryCommand;
use crate::mysql::handler::query::query_result::{Or, QueryResult};

/// 查询系统参数, 仅返回单个值
pub fn query_system_var(conn: &mut Connection, name: &str) -> Result<Option<Value>> {
    query_first(conn, format!("SELECT @@{}", name)).map(|row| row.map(from_row))
}

/// 查询sql并返回第一行数据
pub fn query_first<T: AsRef<str>>(conn: &mut Connection, query: T) -> Result<Option<Row>> {
    query_iter(conn, query)?.next().transpose()
}

/// 执行查询sql，并返回QueryResult用于迭代结果集
pub fn query_iter<T: AsRef<str>>(conn: &mut Connection, query: T) -> Result<QueryResult<'_, Text>> {
    let meta = send_query(conn, query.as_ref())?;
    Ok(QueryResult::new(ConnMut::Mut(conn), meta))
}

/// 执行查询sql, 并丢弃所有结果
pub fn query_drop<T: AsRef<str>>(conn: &mut Connection, query: T) -> Result<()> {
    // 执行sql并丢弃结果
    // QueryResult::drop会调用next获取剩余的result set数据包并丢弃
    query_iter(conn, query).map(drop)
}

/// 发送query command, 处理返回结果
/// 当返回结果为ok时，返回OkPacket
/// 当返回结果为result set时，返回column list
fn send_query(conn: &mut Connection, query: &str) -> Result<Or<Vec<Column>, OkPacket<'static>>> {
    conn.write_command(&QueryCommand::new(query))?;
    conn.handle_result_set()
}

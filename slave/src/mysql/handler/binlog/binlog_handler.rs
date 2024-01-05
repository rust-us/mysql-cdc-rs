use mysql_common::packets::binlog_request::BinlogRequest;
use rand::{Rng, RngCore};
use uuid::Uuid;

use crate::mysql::conn::connection::Connection;
use crate::mysql::error::Result;
use crate::mysql::handler::binlog::binlog_stream::BinlogStream;
use crate::mysql::handler::query::query_handler;

/// 从binlog当前时刻的最后位置开始订阅binlog
pub fn get_binlog_at_last(mut conn: Connection) -> Result<BinlogStream> {
    // 获取当前binlog的文件名和最后的pos
    let last_pos = get_binlog_end_position(&mut conn)?;
    get_binlog(conn, generate_client_id(), last_pos.0, last_pos.1)
}

/// 从binlog的指定位置开始订阅binlog
pub fn get_binlog_at_pos(
    mut conn: Connection,
    filename: Vec<u8>,
    pos: u64,
) -> Result<BinlogStream> {
    get_binlog(conn, generate_client_id(), filename, pos)
}

/// 获取当前时刻binlog的最后点位
/// (binlog_filename, position)
pub fn get_binlog_end_position(conn: &mut Connection) -> Result<(Vec<u8>, u64)> {
    // 获取当前binlog的文件名和最后的pos
    let row = query_handler::query_iter(conn, "show master status")?
        .next()
        .unwrap()?;
    let filename: Vec<u8> = row.get(0).unwrap();
    let position = row.get(1).unwrap();
    Ok((filename, position))
}

/// 生成client id
fn generate_client_id() -> u32 {
    /// 目前使用随机数作为id，还是存在client id冲突的可能性，id相同会导致slave连接冲突
    rand::thread_rng().gen()
}

/// 获取master的serverId
fn get_master_server_id(conn: &mut Connection) -> Result<u64> {
    // 获取db服务器的serverId信息
    let row = query_handler::query_iter(conn, "SHOW VARIABLES LIKE 'SERVER_ID'")?
        .next()
        .unwrap()?;
    Ok(row.get(1).unwrap())
}

/// 获取binlog checksum的类型
/// 目前有CRC32,NONE两种
fn load_binlog_checksum(conn: &mut Connection) -> Result<Vec<u8>> {
    // 获取db服务器的serverId信息
    let row = query_handler::query_iter(conn, "select @@global.binlog_checksum")?
        .next()
        .unwrap()?;
    Ok(row.get(0).unwrap_or(Vec::new()))
}

fn get_binlog(
    mut conn: Connection,
    server_id: u32,
    filename: Vec<u8>,
    pos: u64,
) -> Result<BinlogStream> {
    get_binlog_stream(
        conn,
        BinlogRequest::new(server_id)
            .with_filename(filename)
            .with_pos(pos),
    )
}

/// 进入binlog dump之后不能退出，因此将connection所有权转移到binlogstream内
fn get_binlog_stream(mut conn: Connection, request: BinlogRequest) -> Result<BinlogStream> {
    update_settings(&mut conn)?;
    conn.request_binlog(request)?;
    Ok(BinlogStream::new(conn))
}

/// 继续必要的连接设置
fn update_settings(conn: &mut Connection) -> Result<()> {
    // 设置必要的超时时间，此时如果设置出错可以忽略
    query_handler::query_drop(conn, "set wait_timeout=9999999").unwrap_or(());
    query_handler::query_drop(conn, "set net_write_timeout=7200").unwrap_or(());
    query_handler::query_drop(conn, "set net_read_timeout=7200").unwrap_or(());
    // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送
    query_handler::query_drop(conn, "set names 'binary'").unwrap_or(());

    // mysql5.6针对checksum支持需要设置session变量
    // 如果不设置会出现错误： Slave can not handle replication events with the
    // checksum that master is configured to log
    // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
    // '@@global.binlog_checksum'需要去掉单引号,在mysql 5.6.29下导致master退出
    query_handler::query_drop(
        conn,
        "set @master_binlog_checksum= @@global.binlog_checksum",
    )?;

    // 参考:https://github.com/alibaba/canal/issues/284
    // mysql5.6需要设置slave_uuid避免被server kill链接
    // 设置uuid可以避免部分slave_id冲突的问题
    let uuid = Uuid::default();
    query_handler::query_drop(conn, format!("set @slave_uuid={uuid}")).unwrap_or(());

    // 控制master心跳间隔，每binlog时间发送时master都会重置心跳间隔。
    // 因此，只有当binlog未发送的时间超过心跳间隔时，master才会发送心跳信号。
    // 注意：mysql文档说明心跳为毫秒ms，实际set时单位为纳秒ns, 此处转为ns值
    let period = super::MASTER_HEARTBEAT_PERIOD_MILLISECONDS as u64 * 1000 * 1000;
    query_handler::query_drop(conn, format!("SET @master_heartbeat_period={period}",))
        .unwrap_or(());
    Ok(())
}

#[cfg(test)]
mod test {
    use binlog::events::event::Event;
    use binlog::events::event_factory::EventFactory;
    use common::err::DecodeError::ReError;

    use crate::mysql::conn::connection::Connection;
    use crate::mysql::conn::opts::Opts;
    use crate::mysql::handler::binlog::binlog_handler;

    #[test]
    fn test() {
        let url = "mysql://127.0.0.1:3306?user=root&password=123456";
        let mut conn = Connection::new(Opts::from_url(url).unwrap()).unwrap();

        // let mut binlogs = binlog_handler::get_binlog_at_last(conn).unwrap();
        let filename: Vec<u8> = "binlog.000712".as_bytes().to_vec();
        let position = 4;
        let mut binlogs = binlog_handler::get_binlog_at_pos(conn, filename, position).unwrap();
        println!("aaaaa");

        let factory = EventFactory::new(true);
        factory.parser_iter_with_block(binlogs);
    }
}

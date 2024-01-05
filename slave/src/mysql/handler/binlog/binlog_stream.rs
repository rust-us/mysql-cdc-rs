use mysql_common::io::ParseBuf;
use mysql_common::packets::{ErrPacket, NetworkStreamTerminator, OkPacketDeserializer};

use crate::mysql::conn::connection::Connection;
use crate::mysql::error::DriverError;
use crate::mysql::error::Error;
use crate::mysql::error::Result;
use crate::mysql::handler::query::query_handler;

pub struct BinlogStream {
    // 进入binlog dump之后不能退出，因此将connection所有权转移到binlogstream内
    conn: Option<Connection>,
}

impl BinlogStream {
    pub(in crate::mysql) fn new(conn: Connection) -> Self {
        BinlogStream { conn: Some(conn) }
    }

    /// 关闭binlog stream
    pub fn shutdown(&mut self) -> bool {
        let result = match self.conn.as_mut() {
            Some(conn) => {
                // 先执行关闭
                let _ = conn.shutdown();
                // 主库不发送binlog日志时不检查dump连接状态，因此可能出现关闭后依然在服务端保持的情况
                // 此处新建一个普通连接并发送kill命令将dump连接关闭
                match Connection::fork(&conn) {
                    Ok(mut fork_conn) => {
                        BinlogStream::kill_dump(&mut fork_conn, conn.connection_id())
                    }
                    _ => false,
                }
            }
            _ => false,
        };
        self.conn = None;
        result
    }

    /// kill 连接，用于主动关闭dump
    /// true 执行成功
    fn kill_dump(conn: &mut Connection, conn_id: u32) -> bool {
        match query_handler::query_drop(conn, format!("KILL CONNECTION {}", conn_id)) {
            Ok(_) => true,
            _ => false,
        }
    }
}

impl Drop for BinlogStream {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Iterator for BinlogStream {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let conn = self.conn.as_mut()?;

        let packet = match conn.read_packet() {
            Ok(packet) => packet,
            Err(err) => {
                return Some(Err(err));
            }
        };

        let first_byte = packet.first().copied();

        if first_byte == Some(255) {
            if let Ok(ErrPacket::Error(err)) =
                ParseBuf(&packet).parse(conn.get_copy_capability_flags())
            {
                return Some(Err(Error::MySqlError(From::from(err))));
            }
        }

        if first_byte == Some(254)
            && packet.len() < 8
            && ParseBuf(&packet)
                .parse::<OkPacketDeserializer<NetworkStreamTerminator>>(
                    conn.get_copy_capability_flags(),
                )
                .is_ok()
        {
            return None;
        }

        if first_byte == Some(0) {
            // data第一个字节为固定的00，后续数据才是真正的event内容
            Some(Ok(packet[1..].to_vec()))
        } else {
            Some(Err(DriverError::UnexpectedPacket.into()))
        }
    }
}

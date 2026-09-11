//! `XA_PREPARE_LOG_EVENT`(事件号 38)解析。
//!
//! XA 两阶段提交中，`XA PREPARE` 使事务进入 prepared 状态，
//! 该事件是 XA 事务的边界事件(类比 `Xid`)。
//!
//! # 布局(已用真实 binlog 字节验证)
//!
//! ```text
//! post-header:
//!   one_phase     (1)  0 = XA PREPARE / XA COMMIT 两阶段; 1 = XA COMMIT|ROLLBACK ... ONE PHASE
//!   format_id     (4)  XID 的 formatId，小端
//!   gtrid_length  (4)  全局事务标识长度，小端
//!   bqual_length  (4)  分支限定符长度，小端
//! body:
//!   xid_data      (gtrid_length + bqual_length)  gtrid 紧接 bqual
//! checksum        (4)
//! ```
//!
//! 真实样本(`XA START 'x1'` → `XA PREPARE 'x1'`)的事件体:
//! `00 | 01 00 00 00 | 02 00 00 00 | 00 00 00 00 | 78 31 | 33 ac fc ad`
//! 即 one_phase=0、format_id=1、gtrid_len=2、bqual_len=0、gtrid=`x1`。

use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;
use serde::Serialize;

/// `XA_PREPARE_LOG_EVENT` 的解析结果。
///
/// XID 由 `format_id`、`gtrid`、`bqual` 三者唯一确定，
/// 其字符串形式与 SQL 中的 `'gtrid', 'bqual', format_id` 对应。
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct XaPrepareLogEvent {
    /// 0 = 两阶段 `XA PREPARE`；1 = `... ONE PHASE`
    pub one_phase: bool,

    /// XID 的 formatId
    pub format_id: u32,

    /// gtrid 长度
    pub gtrid_length: u32,

    /// bqual 长度
    pub bqual_length: u32,

    /// 全局事务标识(原始字节，MySQL 允许非 UTF-8)
    pub gtrid: Vec<u8>,

    /// 分支限定符(原始字节)
    pub bqual: Vec<u8>,
}

impl XaPrepareLogEvent {
    /// 是否两阶段(`XA PREPARE` / 中间态)
    pub fn is_two_phase(&self) -> bool {
        !self.one_phase
    }

    /// gtrid 的可读形式(非 UTF-8 字节用 `\xNN` 转义)
    pub fn gtrid_str(&self) -> String {
        escape_xid_bytes(&self.gtrid)
    }

    /// bqual 的可读形式
    pub fn bqual_str(&self) -> String {
        escape_xid_bytes(&self.bqual)
    }

    /// XID 文本表示: `gtrid/bqual/format_id`(与 MySQL 的 XID 打印格式一致)
    pub fn xid_str(&self) -> String {
        format!("{}/{}/{}", self.gtrid_str(), self.bqual_str(), self.format_id)
    }
}

fn escape_xid_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => bytes
            .iter()
            .map(|b| format!("\\x{:02x}", b))
            .collect::<String>(),
    }
}

/// 解析 `XA_PREPARE_LOG_EVENT` 的事件体(不含 19 字节 common header)。
///
/// `body` 允许包含末尾 4 字节校验和，解析时只消费 post-header 与 XID 数据。
pub fn parse_xa_prepare_log_event_body(body: &[u8]) -> Result<XaPrepareLogEvent, ReError> {
    // post-header = 1 + 4 + 4 + 4
    if body.len() < 13 {
        return Err(ReError::String(format!(
            "XA_PREPARE_LOG_EVENT body too short: {} bytes, need at least 13",
            body.len()
        )));
    }

    let mut cursor = Cursor::new(body);

    let one_phase = cursor.read_u8()? != 0;
    let format_id = cursor.read_u32::<LittleEndian>()?;
    let gtrid_length = cursor.read_u32::<LittleEndian>()?;
    let bqual_length = cursor.read_u32::<LittleEndian>()?;

    let xid_length = gtrid_length as usize + bqual_length as usize;
    let remaining = body.len() - cursor.position() as usize;
    if xid_length > remaining {
        return Err(ReError::String(format!(
            "XA_PREPARE_LOG_EVENT xid data truncated: declared {} bytes (gtrid {} + bqual {}), \
             but only {} bytes remain",
            xid_length, gtrid_length, bqual_length, remaining
        )));
    }

    let mut xid_data = vec![0u8; xid_length];
    std::io::Read::read_exact(&mut cursor, &mut xid_data)?;

    let gtrid = xid_data[..gtrid_length as usize].to_vec();
    let bqual = xid_data[gtrid_length as usize..].to_vec();

    Ok(XaPrepareLogEvent {
        one_phase,
        format_id,
        gtrid_length,
        bqual_length,
        gtrid,
        bqual,
    })
}

/// 从游标(已定位到事件体起点)解析，供解码器直接调用。
pub fn parse_xa_prepare_log_event(
    cursor: &mut Cursor<&[u8]>,
) -> Result<XaPrepareLogEvent, ReError> {
    let position = cursor.position() as usize;
    let data: &[u8] = cursor.get_ref();
    let body = data
        .get(position..)
        .ok_or_else(|| ReError::String("XA_PREPARE_LOG_EVENT cursor out of range".to_string()))?;
    parse_xa_prepare_log_event_body(body)
}

/// 便于测试/构造
impl Default for XaPrepareLogEvent {
    fn default() -> Self {
        XaPrepareLogEvent {
            one_phase: false,
            format_id: 0,
            gtrid_length: 0,
            bqual_length: 0,
            gtrid: Vec::new(),
            bqual: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        hex::decode(compact).unwrap()
    }

    /// 真实样本: `XA START 'x1'; ...; XA PREPARE 'x1';`(MySQL 8.0.39)
    /// 事件体 19 字节 = post-header 13 + xid 2 + 校验和 4
    #[test]
    fn test_parse_real_xa_prepare() {
        let body = hex("00 01 00 00 00 02 00 00 00 00 00 00 00 78 31 33 ac fc ad");
        assert_eq!(19, body.len());

        let event = parse_xa_prepare_log_event_body(&body).unwrap();

        assert!(!event.one_phase, "XA PREPARE is two-phase");
        assert!(event.is_two_phase());
        assert_eq!(1, event.format_id);
        assert_eq!(2, event.gtrid_length);
        assert_eq!(0, event.bqual_length);
        assert_eq!(b"x1".to_vec(), event.gtrid);
        assert!(event.bqual.is_empty());
        assert_eq!("x1", event.gtrid_str());
        assert_eq!("", event.bqual_str());
        assert_eq!("x1//1", event.xid_str());
    }

    /// `... ONE PHASE` 时 one_phase = 1
    #[test]
    fn test_one_phase_flag() {
        let body = hex("01 01 00 00 00 02 00 00 00 02 00 00 00 78 31 62 71 00 00");
        let event = parse_xa_prepare_log_event_body(&body).unwrap();

        assert!(event.one_phase);
        assert!(!event.is_two_phase());
        assert_eq!(2, event.gtrid_length);
        assert_eq!(2, event.bqual_length);
        assert_eq!(b"x1".to_vec(), event.gtrid);
        assert_eq!(b"bq".to_vec(), event.bqual);
        assert_eq!("x1/bq/1", event.xid_str());
    }

    /// 非 UTF-8 的 gtrid 需要可读转义，而不是解析失败
    #[test]
    fn test_non_utf8_gtrid_is_escaped() {
        let body = hex("00 01 00 00 00 02 00 00 00 00 00 00 00 ff fe");
        let event = parse_xa_prepare_log_event_body(&body).unwrap();
        assert_eq!("\\xff\\xfe", event.gtrid_str());
    }

    #[test]
    fn test_truncated_body_is_rejected() {
        // 不足 post-header
        assert!(parse_xa_prepare_log_event_body(&hex("00 01 00 00 00")).is_err());
        // 声明 gtrid=10 但只有 2 字节数据
        assert!(parse_xa_prepare_log_event_body(&hex(
            "00 01 00 00 00 0a 00 00 00 00 00 00 00 78 31"
        ))
        .is_err());
    }
}

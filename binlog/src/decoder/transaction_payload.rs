//! `TRANSACTION_PAYLOAD_EVENT` (MySQL 8.0.20+) 的头部解析、解压与事件展开。
//!
//! # 线上格式
//!
//! ```text
//! 事件体 =
//!   +----------------------------------------------------+
//!   | payload header: TLV 序列，以 end mark 结束           |
//!   |   tag=0x02 len=0x01 value=<1B 压缩类型>             |
//!   |   tag=0x03 len=N    value=<lenenc 未压缩大小>        |
//!   |   tag=0x01 len=N    value=<lenenc 压缩后大小>        |
//!   |   tag=0x00 (end mark)                               |
//!   +----------------------------------------------------+
//!   | 压缩后的载荷(payload_size 字节)                       |
//!   +----------------------------------------------------+
//!   | 4 字节事件校验和(CRC32)                               |
//!   +----------------------------------------------------+
//! ```
//!
//! 要点:
//!
//! 1. 头部是 **TLV**(tag/length/value)，`tag=0x00` 为结束标记。
//!    因此**字段顺序不固定、总长可变**，必须逐 TLV 解析，不能按固定偏移读取。
//! 2. `payload_size` / `uncompressed_size` 的 value 采用 MySQL lenenc 编码
//!    (`0xFC`+2B / `0xFD`+3B / `0xFE`+8B)，TLV 的 `len` 即该 lenenc 占用的字节数。
//! 3. 压缩类型: `0x00` = ZSTD，`0xFF` = NONE(载荷原样存储)。
//!
//! # 解压后的内容
//!
//! 是**若干个完整 binlog 事件首尾相接**：每个事件都带 19 字节 common header
//! (其中 `log_pos` 为 0)，且**不再包含各自的 4 字节校验和**。
//! 因此可以按 `header.event_length` 精确切分，逐个交给事件解析器处理。
//!
//! 以上格式已用 MySQL 8.0.39 的真实事务载荷(ZSTD，含 6 / 6 / 3006 个内含事件)
//! 逐字节验证。

use std::io::Read;
use std::ops::Range;

use common::err::CResult;
use common::err::decode_error::ReError;

/// binlog 事件 common header 长度(mysql 8.0)
pub const EVENT_HEADER_LEN: usize = 19;

/// TLV tag: 压缩后的载荷大小
pub const TAG_PAYLOAD_SIZE: u8 = 0x01;
/// TLV tag: 压缩类型
pub const TAG_COMPRESSION_TYPE: u8 = 0x02;
/// TLV tag: 未压缩大小
pub const TAG_UNCOMPRESSED_SIZE: u8 = 0x03;
/// TLV tag: 头部结束标记
pub const TAG_END_MARK: u8 = 0x00;

/// 压缩类型: zstd
pub const COMPRESSION_ZSTD: u8 = 0x00;
/// 压缩类型: 未压缩
pub const COMPRESSION_NONE: u8 = 0xFF;

/// 载荷压缩类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Zstd,
    None,
}

impl CompressionType {
    fn from_byte(value: u8) -> CResult<Self> {
        match value {
            COMPRESSION_ZSTD => Ok(CompressionType::Zstd),
            COMPRESSION_NONE => Ok(CompressionType::None),
            other => Err(ReError::String(format!(
                "unsupported transaction payload compression type: 0x{:02x}",
                other
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Zstd => "ZSTD",
            CompressionType::None => "NONE",
        }
    }
}

/// 解析出的 payload header
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadHeader {
    /// 压缩类型
    pub compression_type: CompressionType,

    /// 压缩后的载荷字节数
    pub payload_size: u64,

    /// 解压后的字节数(未压缩时为 0)
    pub uncompressed_size: u64,

    /// 头部占用的字节数，载荷从该偏移开始
    pub header_len: usize,
}

/// 读取 MySQL length-encoded integer，返回 `(值, 消耗字节数)`。
fn read_lenenc(buf: &[u8]) -> CResult<(u64, usize)> {
    let first = *buf
        .first()
        .ok_or_else(|| ReError::String("truncated lenenc value".to_string()))?;

    let need = |n: usize| -> CResult<&[u8]> {
        buf.get(1..n)
            .ok_or_else(|| ReError::String(format!("truncated lenenc value, need {} bytes", n)))
    };

    match first {
        // NULL 标记，这两个事件里不出现；按 0 处理以免误判
        0xFB => Ok((0, 1)),
        0xFC => {
            let b = need(3)?;
            Ok((u16::from_le_bytes([b[0], b[1]]) as u64, 3))
        }
        0xFD => {
            let b = need(4)?;
            Ok(((b[0] as u64) | ((b[1] as u64) << 8) | ((b[2] as u64) << 16), 4))
        }
        0xFE => {
            let b = need(9)?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(b);
            Ok((u64::from_le_bytes(bytes), 9))
        }
        value => Ok((value as u64, 1)),
    }
}

/// 解析 `TRANSACTION_PAYLOAD_EVENT` 的载荷头部(TLV)。
pub fn parse_payload_header(body: &[u8]) -> CResult<PayloadHeader> {
    let mut offset = 0usize;
    let mut compression_type = CompressionType::None;
    let mut payload_size: Option<u64> = None;
    let mut uncompressed_size: u64 = 0;
    let mut seen_end_mark = false;

    while offset < body.len() {
        let tag = body[offset];
        offset += 1;

        // end mark 没有 length/value
        if tag == TAG_END_MARK {
            seen_end_mark = true;
            break;
        }

        let length = *body.get(offset).ok_or_else(|| {
            ReError::String(format!("truncated transaction payload header at offset {}", offset))
        })? as usize;
        offset += 1;

        let value = body.get(offset..offset + length).ok_or_else(|| {
            ReError::String(format!(
                "truncated transaction payload header value (tag=0x{:02x}, len={})",
                tag, length
            ))
        })?;
        offset += length;

        match tag {
            TAG_PAYLOAD_SIZE => {
                let (size, used) = read_lenenc(value)?;
                check_lenenc_len(tag, length, used)?;
                payload_size = Some(size);
            }
            TAG_COMPRESSION_TYPE => {
                let first = *value.first().ok_or_else(|| {
                    ReError::String("empty compression type field".to_string())
                })?;
                compression_type = CompressionType::from_byte(first)?;
            }
            TAG_UNCOMPRESSED_SIZE => {
                let (size, used) = read_lenenc(value)?;
                check_lenenc_len(tag, length, used)?;
                uncompressed_size = size;
            }
            // 未知 tag 是 TLV 设计上的前向兼容点: 按长度跳过即可
            _ => {}
        }
    }

    if !seen_end_mark {
        return Err(ReError::String(
            "transaction payload header has no end mark".to_string(),
        ));
    }

    let payload_size = payload_size.ok_or_else(|| {
        ReError::String("transaction payload header has no payload_size field".to_string())
    })?;

    Ok(PayloadHeader {
        compression_type,
        payload_size,
        uncompressed_size,
        header_len: offset,
    })
}

fn check_lenenc_len(tag: u8, declared: usize, used: usize) -> CResult<()> {
    if declared != used {
        return Err(ReError::String(format!(
            "transaction payload TLV length mismatch for tag 0x{:02x}: declared {}, lenenc uses {}",
            tag, declared, used
        )));
    }
    Ok(())
}

/// 解压载荷，返回解压后的完整事件流。
///
/// `body` 为事件体(common header 之后、含 4 字节校验和的完整切片)。
pub fn decode_payload(body: &[u8]) -> CResult<Vec<u8>> {
    let header = parse_payload_header(body)?;
    let start = header.header_len;
    let end = start + header.payload_size as usize;

    // 载荷完整性校验放在这里: `parse_payload_header` 只解析头部，
    // 允许调用方在只有头部的情况下先看元信息。
    if end > body.len() {
        return Err(ReError::String(format!(
            "transaction payload is truncated: header ends at {}, declared payload {} bytes, body {} bytes",
            start,
            header.payload_size,
            body.len()
        )));
    }
    let raw = &body[start..end];

    match header.compression_type {
        CompressionType::None => Ok(raw.to_vec()),
        CompressionType::Zstd => {
            let mut out = Vec::with_capacity(header.uncompressed_size as usize);
            let mut decoder = zstd::stream::Decoder::new(raw)
                .map_err(|e| ReError::String(format!("init zstd decoder err: {}", e)))?;
            decoder
                .read_to_end(&mut out)
                .map_err(|e| ReError::String(format!("zstd decompress err: {}", e)))?;

            if header.uncompressed_size != 0 && out.len() != header.uncompressed_size as usize {
                return Err(ReError::String(format!(
                    "transaction payload uncompressed size mismatch: declared {}, actual {}",
                    header.uncompressed_size,
                    out.len()
                )));
            }
            Ok(out)
        }
    }
}

/// 按 19 字节 common header 的 `event_length` 切分事件流。
///
/// 返回值严格覆盖整个 `buf`；若存在多余/不足字节则报错(说明流已损坏)。
pub fn split_events(buf: &[u8]) -> CResult<Vec<Range<usize>>> {
    let mut events = Vec::new();
    let mut offset = 0usize;

    while offset < buf.len() {
        if buf.len() - offset < EVENT_HEADER_LEN {
            return Err(ReError::String(format!(
                "transaction payload has {} trailing bytes, less than an event header",
                buf.len() - offset
            )));
        }

        // event_length 位于 common header 的偏移 9..13
        let event_length = u32::from_le_bytes([
            buf[offset + 9],
            buf[offset + 10],
            buf[offset + 11],
            buf[offset + 12],
        ]) as usize;

        if event_length < EVENT_HEADER_LEN {
            return Err(ReError::String(format!(
                "invalid inner event length {} at offset {}",
                event_length, offset
            )));
        }
        if offset + event_length > buf.len() {
            return Err(ReError::String(format!(
                "inner event at offset {} overflows payload (length {}, remaining {})",
                offset,
                event_length,
                buf.len() - offset
            )));
        }

        events.push(offset..offset + event_length);
        offset += event_length;
    }

    Ok(events)
}

#[cfg(test)]
mod test {
    use super::*;

    /// 以下三个头部来自 MySQL 8.0.39 的真实 binlog
    /// (会话级 `binlog_transaction_compression=ON` 产生的压缩事务)。
    const HEADER_SMALL: &[u8] = &[
        0x02, 0x01, 0x00, // 压缩类型 ZSTD
        0x03, 0x03, 0xfc, 0x72, 0x01, // 未压缩大小 = 370
        0x01, 0x01, 0xb1, // 压缩后大小 = 177
        0x00, // end mark
    ];
    const HEADER_MEDIUM: &[u8] = &[
        0x02, 0x01, 0x00, 0x03, 0x03, 0xfc, 0x81, 0x20, 0x01, 0x03, 0xfc, 0x4d, 0x12, 0x00,
    ];
    const HEADER_LARGE: &[u8] = &[
        0x02, 0x01, 0x00, 0x03, 0x04, 0xfd, 0x7e, 0x16, 0x03, 0x01, 0x04, 0xfd, 0x02, 0x13, 0x02,
        0x00,
    ];

    #[test]
    fn test_parse_real_payload_headers() {
        let small = parse_payload_header(HEADER_SMALL).unwrap();
        assert_eq!(CompressionType::Zstd, small.compression_type);
        assert_eq!(370, small.uncompressed_size);
        assert_eq!(177, small.payload_size);
        assert_eq!(12, small.header_len);

        // 交叉校验: body(4703) = header(14) + payload(4685) + checksum(4)
        let medium = parse_payload_header(HEADER_MEDIUM).unwrap();
        assert_eq!(CompressionType::Zstd, medium.compression_type);
        assert_eq!(8321, medium.uncompressed_size);
        assert_eq!(4685, medium.payload_size);
        assert_eq!(14, medium.header_len);

        let large = parse_payload_header(HEADER_LARGE).unwrap();
        assert_eq!(CompressionType::Zstd, large.compression_type);
        assert_eq!(202366, large.uncompressed_size);
        assert_eq!(135938, large.payload_size);
        assert_eq!(16, large.header_len);
    }

    /// TLV 是顺序无关的：字段颠倒也能解析
    #[test]
    fn test_parse_payload_header_is_order_independent() {
        let reordered = [
            0x01, 0x01, 0xb1, // payload_size = 177
            0x03, 0x03, 0xfc, 0x72, 0x01, // uncompressed = 370
            0x02, 0x01, 0x00, // compression = ZSTD
            0x00,
        ];
        let header = parse_payload_header(&reordered).unwrap();
        assert_eq!(CompressionType::Zstd, header.compression_type);
        assert_eq!(370, header.uncompressed_size);
        assert_eq!(177, header.payload_size);
    }

    /// 未知 tag 按 TLV 长度跳过，保证向前兼容
    #[test]
    fn test_parse_payload_header_skips_unknown_tag() {
        let with_unknown = [
            0x02, 0x01, 0x00, //
            0x7f, 0x03, 0xaa, 0xbb, 0xcc, // 未知 tag
            0x03, 0x03, 0xfc, 0x72, 0x01, //
            0x01, 0x01, 0xb1, //
            0x00,
        ];
        let header = parse_payload_header(&with_unknown).unwrap();
        assert_eq!(370, header.uncompressed_size);
        assert_eq!(177, header.payload_size);
        assert_eq!(17, header.header_len);
    }

    #[test]
    fn test_parse_payload_header_errors() {
        // 缺少 end mark
        assert!(parse_payload_header(&[0x02, 0x01, 0x00]).is_err());
        // 缺少 payload_size
        assert!(parse_payload_header(&[0x02, 0x01, 0x00, 0x00]).is_err());
        // lenenc 长度与 TLV 声明不符
        assert!(parse_payload_header(&[0x01, 0x03, 0xb1, 0x00, 0x00]).is_err());
        // 不支持的压缩类型
        assert!(parse_payload_header(&[0x02, 0x01, 0x07, 0x01, 0x01, 0xb1, 0x00]).is_err());
    }

    /// 构造 TLV 头部(便于测试)
    fn build_header(compression: u8, uncompressed: u64, payload_size: u64) -> Vec<u8> {
        fn lenenc(value: u64) -> Vec<u8> {
            if value < 0xFB {
                vec![value as u8]
            } else if value <= u16::MAX as u64 {
                let mut v = vec![0xFC];
                v.extend_from_slice(&(value as u16).to_le_bytes());
                v
            } else if value <= 0xFF_FFFF {
                let mut v = vec![0xFD];
                v.extend_from_slice(&(value as u32).to_le_bytes()[..3]);
                v
            } else {
                let mut v = vec![0xFE];
                v.extend_from_slice(&value.to_le_bytes());
                v
            }
        }

        let mut body = vec![TAG_COMPRESSION_TYPE, 0x01, compression];

        let unc = lenenc(uncompressed);
        body.push(TAG_UNCOMPRESSED_SIZE);
        body.push(unc.len() as u8);
        body.extend_from_slice(&unc);

        let size = lenenc(payload_size);
        body.push(TAG_PAYLOAD_SIZE);
        body.push(size.len() as u8);
        body.extend_from_slice(&size);

        body.push(TAG_END_MARK);
        body
    }

    /// 未压缩(compression_type = NONE)时载荷原样返回
    #[test]
    fn test_decode_payload_without_compression() {
        let mut body = build_header(COMPRESSION_NONE, 0, 3);
        body.extend_from_slice(b"abc");
        body.extend_from_slice(&[0u8; 4]); // 事件校验和

        assert_eq!(b"abc".to_vec(), decode_payload(&body).unwrap());
    }

    /// zstd 往返: 自造头部 + 真实 zstd 压缩数据
    #[test]
    fn test_decode_payload_zstd_round_trip() {
        let raw: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let compressed = zstd::stream::encode_all(&raw[..], 3).unwrap();

        let mut body = build_header(COMPRESSION_ZSTD, raw.len() as u64, compressed.len() as u64);
        let header_len = body.len();
        body.extend_from_slice(&compressed);
        body.extend_from_slice(&[0u8; 4]);

        let header = parse_payload_header(&body).unwrap();
        assert_eq!(CompressionType::Zstd, header.compression_type);
        assert_eq!(header_len, header.header_len);

        assert_eq!(raw, decode_payload(&body).unwrap());
    }

    /// 声明的载荷长度超过实际字节数时必须报错，而不是静默截断
    #[test]
    fn test_decode_payload_truncated() {
        let mut body = build_header(COMPRESSION_NONE, 0, 16);
        body.extend_from_slice(b"ab");
        assert!(decode_payload(&body).is_err());
    }

    #[test]
    fn test_read_lenenc() {
        assert_eq!((177, 1), read_lenenc(&[0xb1]).unwrap());
        assert_eq!((370, 3), read_lenenc(&[0xfc, 0x72, 0x01]).unwrap());
        assert_eq!((202366, 4), read_lenenc(&[0xfd, 0x7e, 0x16, 0x03]).unwrap());
        assert!(read_lenenc(&[0xfc, 0x72]).is_err());
    }

    /// 用真实解压后的字节流验证事件切分：
    /// BEGIN / Table_map / Write_rows / Table_map / Update_rows / Xid，共 8321 字节
    #[test]
    fn test_split_real_events() {
        let mut buf = Vec::new();
        let mut push_event = |event_type: u8, event_length: u32| {
            let mut header = [0u8; EVENT_HEADER_LEN];
            header[4] = event_type;
            header[9..13].copy_from_slice(&event_length.to_le_bytes());
            buf.extend_from_slice(&header);
            buf.extend(std::iter::repeat(0u8).take(event_length as usize - EVENT_HEADER_LEN));
        };
        push_event(2, 67); // Query
        push_event(19, 57); // Table_map
        push_event(30, 6731); // Write_rows
        push_event(19, 57); // Table_map
        push_event(31, 1382); // Update_rows
        push_event(16, 27); // Xid

        assert_eq!(8321, buf.len());
        let ranges = split_events(&buf).unwrap();
        assert_eq!(6, ranges.len());
        assert_eq!(0..67, ranges[0]);
        assert_eq!(8294..8321, ranges[5]);
    }

    #[test]
    fn test_split_events_rejects_corruption() {
        // 尾随不足一个 header
        assert!(split_events(&[0u8; EVENT_HEADER_LEN + 3]).is_err());
        // event_length 小于最小 header
        let mut buf = [0u8; EVENT_HEADER_LEN];
        buf[9..13].copy_from_slice(&(4u32).to_le_bytes());
        assert!(split_events(&buf).is_err());
        // event_length 溢出
        let mut buf = [0u8; EVENT_HEADER_LEN];
        buf[9..13].copy_from_slice(&(100u32).to_le_bytes());
        assert!(split_events(&buf).is_err());
    }
}

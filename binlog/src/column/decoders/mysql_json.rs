//! MySQL 二进制 JSON (`JSONB`) 解码，输出 **JSON 文本**。
//!
//! # 为什么输出文本而不是 `serde_json::Value`
//!
//! MySQL 会把 JSON 中的小数常量(如 `3.5`)存为 opaque + `NEWDECIMAL`。
//! 若把它转成 `serde_json::Value`，在没有 `arbitrary_precision` 特性时
//! 数字会退化为 `f64`，`123456789012345678901234567890.123456789`
//! 这类值会被**静默截断**。CDC 的首要要求是数据保真，因此这里直接构造
//! JSON 文本，小数按原始位数精确输出。
//!
//! # 为什么必须自己实现
//!
//! 仓库原有的 `column/decoders/json.rs::parse_mysql_json_binary` 使用了一套
//! **错误的类型码表**(把 `0x00` 当 Null、`0x01/0x02` 当布尔、用 varint 读 count/size)，
//! 而真实格式中 `0x00` 是 small object、`count/size` 是定长 2/4 字节。
//!
//! # 格式(已用真实 binlog 字节逐字段验证)
//!
//! | code | 含义 | | code | 含义 |
//! |---|---|---|---|---|
//! | 0x00 | small object | | 0x06 | uint16 |
//! | 0x01 | large object | | 0x07 | int32 |
//! | 0x02 | small array | | 0x08 | uint32 |
//! | 0x03 | large array | | 0x09 | int64 |
//! | 0x04 | literal (0/null,1/true,2/false) | | 0x0a | uint64 |
//! | 0x05 | int16 | | 0x0b | double |
//! | | | | 0x0c | string |
//! | | | | 0x0f | opaque |
//!
//! 容器布局: `type(1) | count | size | key_entries | value_entries | keys | 离线值`
//!
//! - small: `count/size` 各 2 字节；键条目 `(key_offset u16, key_len u16)`；
//!   值条目 `(type u8, inline_or_offset u16)`
//! - large: `count/size` 各 4 字节；键条目 `(key_offset u32, key_len u16)`；
//!   值条目 `(type u8, inline_or_offset u32)`
//! - **偏移基准 = 类型字节之后的第一个字节**
//! - 内联类型直接存值(`literal` 1 字节、`int16/uint16` 2 字节、`int32/uint32` 4 字节)
//! - 离线值**不再重复类型字节**: string 处直接是 `lenenc 长度 + UTF-8`，
//!   嵌套容器处直接是 `count/size/...`
//! - opaque: `custom_type(1) | length(lenenc) | 负载`；其中 `NEWDECIMAL(246)`
//!   的负载是 `precision(1) | scale(1) | packed-decimal`

use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use common::err::decode_error::ReError;

/// small object
pub const JSONB_TYPE_SMALL_OBJECT: u8 = 0x00;
/// large object
pub const JSONB_TYPE_LARGE_OBJECT: u8 = 0x01;
/// small array
pub const JSONB_TYPE_SMALL_ARRAY: u8 = 0x02;
/// large array
pub const JSONB_TYPE_LARGE_ARRAY: u8 = 0x03;
/// literal (null / true / false)
pub const JSONB_TYPE_LITERAL: u8 = 0x04;
pub const JSONB_TYPE_INT16: u8 = 0x05;
pub const JSONB_TYPE_UINT16: u8 = 0x06;
pub const JSONB_TYPE_INT32: u8 = 0x07;
pub const JSONB_TYPE_UINT32: u8 = 0x08;
pub const JSONB_TYPE_INT64: u8 = 0x09;
pub const JSONB_TYPE_UINT64: u8 = 0x0a;
pub const JSONB_TYPE_DOUBLE: u8 = 0x0b;
pub const JSONB_TYPE_STRING: u8 = 0x0c;
/// opaque(自定义类型)
pub const JSONB_TYPE_OPAQUE: u8 = 0x0f;

const LITERAL_NULL: u8 = 0x00;
const LITERAL_TRUE: u8 = 0x01;
const LITERAL_FALSE: u8 = 0x02;

/// MySQL 自定义类型: `NEWDECIMAL`
const MYSQL_TYPE_NEWDECIMAL: u8 = 246;

/// `dig2bytes[n]` = n 个十进制位需要的字节数
const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
/// 每个完整组的十进制位数
const DIG_PER_DEC1: usize = 9;

/// 解码一段完整的二进制 JSON，返回 JSON 文本。
///
/// 例如 `JSON_OBJECT('k0',0,...)` 会返回 `{"k0":0,...}`。
pub fn decode_mysql_json_text(data: &[u8]) -> Result<String, ReError> {
    let type_code = *data
        .first()
        .ok_or_else(|| ReError::String("empty binary JSON value".to_string()))?;

    read_value(type_code, &data[1..], 0)
}

/// 读取一个"已知类型"的值并渲染为 JSON 文本。
///
/// * `body`: 该值的负载。顶层值是类型字节之后的全部数据；
///   离线值是从偏移处开始的数据(**不含类型字节**)。
/// * `base`: 仅用于诊断信息。
fn read_value(type_code: u8, body: &[u8], base: usize) -> Result<String, ReError> {
    match type_code {
        JSONB_TYPE_SMALL_OBJECT => read_object(body, false, base),
        JSONB_TYPE_LARGE_OBJECT => read_object(body, true, base),
        JSONB_TYPE_SMALL_ARRAY => read_array(body, false, base),
        JSONB_TYPE_LARGE_ARRAY => read_array(body, true, base),

        JSONB_TYPE_LITERAL => match body.first().copied() {
            Some(LITERAL_NULL) => Ok("null".to_string()),
            Some(LITERAL_TRUE) => Ok("true".to_string()),
            Some(LITERAL_FALSE) => Ok("false".to_string()),
            other => Err(ReError::String(format!(
                "invalid binary JSON literal: {:?}",
                other
            ))),
        },

        JSONB_TYPE_INT16 => Ok(read_i16(body)?.to_string()),
        JSONB_TYPE_UINT16 => Ok(read_u16(body)?.to_string()),
        JSONB_TYPE_INT32 => Ok(read_i32(body)?.to_string()),
        JSONB_TYPE_UINT32 => Ok(read_u32(body)?.to_string()),
        JSONB_TYPE_INT64 => Ok(read_i64(body)?.to_string()),
        JSONB_TYPE_UINT64 => Ok(read_u64(body)?.to_string()),

        JSONB_TYPE_DOUBLE => render_f64(read_f64(body)?),

        JSONB_TYPE_STRING => {
            let (length, used) = read_lenenc(body)?;
            let bytes = body
                .get(used..used + length as usize)
                .ok_or_else(|| truncated("json string", used + length as usize, body.len()))?;
            render_json_string(bytes)
        }

        JSONB_TYPE_OPAQUE => read_opaque(body),

        other => Err(ReError::String(format!(
            "unknown binary JSON type code 0x{:02x} (offset {})",
            other, base
        ))),
    }
}

/// 容器头: 返回 `(count, key_entry_size, value_field_size, header_len)`
fn read_container_header(
    body: &[u8],
    large: bool,
) -> Result<(usize, usize, usize, usize), ReError> {
    let (count, header_len) = if large {
        (read_u32(body)? as usize, 8usize)
    } else {
        (read_u16(body)? as usize, 4usize)
    };

    let value_field_size = if large { 4 } else { 2 };
    let key_entry_size = if large { 6 } else { 4 };

    if count > body.len() {
        return Err(ReError::String(format!(
            "binary JSON element count {} exceeds remaining {} bytes",
            count,
            body.len()
        )));
    }

    Ok((count, key_entry_size, value_field_size, header_len))
}

fn read_object(body: &[u8], large: bool, base: usize) -> Result<String, ReError> {
    let (count, key_entry_size, value_field_size, entries_start) =
        read_container_header(body, large)?;

    let value_entries_start = entries_start + count * key_entry_size;

    let mut out = String::from("{");

    for index in 0..count {
        let key_entry = entries_start + index * key_entry_size;
        let (key_offset, key_length) = if large {
            (
                read_u32_at(body, key_entry)? as usize,
                read_u16_at(body, key_entry + 4)? as usize,
            )
        } else {
            (
                read_u16_at(body, key_entry)? as usize,
                read_u16_at(body, key_entry + 2)? as usize,
            )
        };

        let key_bytes = body
            .get(key_offset..key_offset + key_length)
            .ok_or_else(|| truncated("json object key", key_offset + key_length, body.len()))?;

        let value_entry = value_entries_start + index * (1 + value_field_size);
        let rendered = read_entry(body, value_entry, large, base)?;

        if index > 0 {
            out.push(',');
        }
        out.push_str(&render_json_string(key_bytes)?);
        out.push(':');
        out.push_str(&rendered);
    }

    out.push('}');
    Ok(out)
}

fn read_array(body: &[u8], large: bool, base: usize) -> Result<String, ReError> {
    let (count, _key_entry_size, value_field_size, _entries_start) =
        read_container_header(body, large)?;

    let value_entries_start = if large { 8 } else { 4 };

    let mut out = String::from("[");

    for index in 0..count {
        let value_entry = value_entries_start + index * (1 + value_field_size);
        let rendered = read_entry(body, value_entry, large, base)?;

        if index > 0 {
            out.push(',');
        }
        out.push_str(&rendered);
    }

    out.push(']');
    Ok(out)
}

/// 读取值条目。
///
/// 非内联类型的偏移基准是**当前容器的 `body` 起点**(即其类型字节之后)，
/// 不是 `values_start`，也不是整篇文档起点 —— 已用真实字节验证。
fn read_entry(
    body: &[u8],
    offset: usize,
    large: bool,
    base: usize,
) -> Result<String, ReError> {
    let type_code = *body
        .get(offset)
        .ok_or_else(|| truncated("json value entry type", offset + 1, body.len()))?;

    let raw = if large {
        read_u32_at(body, offset + 1)? as u64
    } else {
        read_u16_at(body, offset + 1)? as u64
    };

    match type_code {
        // 内联类型: 字段本身就是值
        JSONB_TYPE_LITERAL => match (raw & 0xff) as u8 {
            LITERAL_NULL => Ok("null".to_string()),
            LITERAL_TRUE => Ok("true".to_string()),
            LITERAL_FALSE => Ok("false".to_string()),
            other => Err(ReError::String(format!(
                "invalid binary JSON literal {} in value entry",
                other
            ))),
        },
        JSONB_TYPE_INT16 => Ok((raw as u16 as i16).to_string()),
        JSONB_TYPE_UINT16 => Ok((raw as u16).to_string()),
        JSONB_TYPE_INT32 => Ok((raw as u32 as i32).to_string()),
        JSONB_TYPE_UINT32 => Ok((raw as u32).to_string()),

        // 非内联类型: 字段是相对当前容器 body 起点的偏移
        _ => {
            let payload_offset = raw as usize;
            let payload = body
                .get(payload_offset..)
                .ok_or_else(|| truncated("json out-of-line value", payload_offset, body.len()))?;
            read_value(type_code, payload, base + payload_offset)
        }
    }
}

/// opaque: `custom_type(1) | length(lenenc) | payload`
fn read_opaque(body: &[u8]) -> Result<String, ReError> {
    let custom_type = *body
        .first()
        .ok_or_else(|| truncated("json opaque header", 1, 0))?;

    let rest = &body[1..];
    let (length, used) = read_lenenc(rest)?;
    let payload = rest
        .get(used..used + length as usize)
        .ok_or_else(|| truncated("json opaque payload", used + length as usize, rest.len()))?;

    match custom_type {
        MYSQL_TYPE_NEWDECIMAL => decode_new_decimal(payload),
        other => Err(ReError::String(format!(
            "binary JSON opaque value with custom type {} is not supported yet \
             (only NEWDECIMAL/246 is implemented)",
            other
        ))),
    }
}

/// 解码 MySQL packed-decimal(`NEWDECIMAL` 的负载)。
///
/// 负载布局: `precision(1) | scale(1) | packed digits`
///
/// packed digits: 按 9 位一组(每组 4 字节大端)存储，
/// 最高位组与最低位组可能是不足 9 位的部分组(字节数见 `DIG2BYTES`)。
/// 符号位在**第一个字节的最高位**: 置位为正；负数是正数编码的**按位取反**。
///
/// 返回精确的十进制文本(不经 `f64`，避免精度损失)。
fn decode_new_decimal(payload: &[u8]) -> Result<String, ReError> {
    if payload.len() < 2 {
        return Err(ReError::String(format!(
            "packed decimal payload too short: {} bytes",
            payload.len()
        )));
    }

    let precision = payload[0] as usize;
    let scale = payload[1] as usize;
    if precision == 0 || scale > precision {
        return Err(ReError::String(format!(
            "invalid packed decimal precision/scale: {}/{}",
            precision, scale
        )));
    }

    let mut digits = payload[2..].to_vec();

    // 负数: 存储的是正数编码按位取反后的结果
    let negative = digits.first().map(|b| b & 0x80 == 0).unwrap_or(false);
    if negative {
        for byte in digits.iter_mut() {
            *byte = !*byte;
        }
    }
    if let Some(first) = digits.first_mut() {
        *first &= 0x7f; // 去掉符号位
    }

    let intg = precision - scale;
    let intg0 = intg / DIG_PER_DEC1;
    let intg0x = intg % DIG_PER_DEC1;
    let frac0 = scale / DIG_PER_DEC1;
    let frac0x = scale % DIG_PER_DEC1;

    let expected = DIG2BYTES[intg0x] + intg0 * 4 + frac0 * 4 + DIG2BYTES[frac0x];
    if digits.len() != expected {
        return Err(ReError::String(format!(
            "packed decimal size mismatch: precision={} scale={} expects {} bytes but got {}",
            precision,
            scale,
            expected,
            digits.len()
        )));
    }

    let mut offset = 0usize;

    // ---- 整数部分 ----
    let mut int_groups: Vec<u32> = Vec::with_capacity(intg0 + 1);
    if intg0x > 0 {
        let n = DIG2BYTES[intg0x];
        int_groups.push(read_be(&digits[offset..offset + n]));
        offset += n;
    }
    for _ in 0..intg0 {
        int_groups.push(read_be(&digits[offset..offset + 4]));
        offset += 4;
    }

    let all_zero = int_groups.iter().all(|g| *g == 0);
    let mut int_text = String::new();
    for (index, group) in int_groups.iter().enumerate() {
        if index == 0 {
            int_text.push_str(&group.to_string());
        } else {
            int_text.push_str(&format!("{:09}", group));
        }
    }
    if int_text.is_empty() {
        int_text.push('0');
    }

    // ---- 小数部分 ----
    let mut frac_text = String::new();
    for _ in 0..frac0 {
        frac_text.push_str(&format!("{:09}", read_be(&digits[offset..offset + 4])));
        offset += 4;
    }
    if frac0x > 0 {
        let n = DIG2BYTES[frac0x];
        let value = read_be(&digits[offset..offset + n]);
        frac_text.push_str(&format!("{:0width$}", value, width = frac0x));
        offset += n;
    }

    let frac_zero = frac_text.chars().all(|c| c == '0');

    let mut text = String::new();
    if negative && !(all_zero && frac_zero) {
        text.push('-');
    }
    text.push_str(&int_text);
    if scale > 0 {
        text.push('.');
        text.push_str(&frac_text);
    }

    Ok(text)
}

/// 读取大端无符号整数(1..4 字节)
fn read_be(bytes: &[u8]) -> u32 {
    let mut value = 0u32;
    for byte in bytes {
        value = (value << 8) | (*byte as u32);
    }
    value
}

/// 渲染 JSON 字符串(带转义)
fn render_json_string(bytes: &[u8]) -> Result<String, ReError> {
    let text = String::from_utf8_lossy(bytes);
    serde_json::to_string(text.as_ref())
        .map_err(|e| ReError::String(format!("failed to escape JSON string: {}", e)))
}

/// 渲染 f64
fn render_f64(value: f64) -> Result<String, ReError> {
    if !value.is_finite() {
        return Err(ReError::String(format!(
            "binary JSON double is not finite: {}",
            value
        )));
    }
    Ok(serde_json::Number::from_f64(value)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "null".to_string()))
}

fn truncated(what: &str, needed: usize, available: usize) -> ReError {
    ReError::String(format!(
        "truncated binary JSON: {} needs {} bytes but only {} available",
        what, needed, available
    ))
}

/// 读取 MySQL lenenc，返回 `(值, 消耗字节数)`
fn read_lenenc(data: &[u8]) -> Result<(u64, usize), ReError> {
    let first = *data.first().ok_or_else(|| truncated("lenenc", 1, 0))?;

    let need = |n: usize| -> Result<&[u8], ReError> {
        data.get(1..n)
            .ok_or_else(|| truncated("lenenc", n, data.len()))
    };

    match first {
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

fn read_i16(b: &[u8]) -> Result<i16, ReError> {
    Cursor::new(b).read_i16::<LittleEndian>().map_err(io_err)
}
fn read_u16(b: &[u8]) -> Result<u16, ReError> {
    Cursor::new(b).read_u16::<LittleEndian>().map_err(io_err)
}
fn read_i32(b: &[u8]) -> Result<i32, ReError> {
    Cursor::new(b).read_i32::<LittleEndian>().map_err(io_err)
}
fn read_u32(b: &[u8]) -> Result<u32, ReError> {
    Cursor::new(b).read_u32::<LittleEndian>().map_err(io_err)
}
fn read_i64(b: &[u8]) -> Result<i64, ReError> {
    Cursor::new(b).read_i64::<LittleEndian>().map_err(io_err)
}
fn read_u64(b: &[u8]) -> Result<u64, ReError> {
    Cursor::new(b).read_u64::<LittleEndian>().map_err(io_err)
}
fn read_f64(b: &[u8]) -> Result<f64, ReError> {
    Cursor::new(b).read_f64::<LittleEndian>().map_err(io_err)
}

fn read_u16_at(b: &[u8], offset: usize) -> Result<u16, ReError> {
    let s = b
        .get(offset..offset + 2)
        .ok_or_else(|| truncated("u16", offset + 2, b.len()))?;
    Ok(u16::from_le_bytes([s[0], s[1]]))
}
fn read_u32_at(b: &[u8], offset: usize) -> Result<u32, ReError> {
    let s = b
        .get(offset..offset + 4)
        .ok_or_else(|| truncated("u32", offset + 4, b.len()))?;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn io_err(e: std::io::Error) -> ReError {
    ReError::String(format!("binary JSON read error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn hex(s: &str) -> Vec<u8> {
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        hex::decode(compact).expect("valid hex fixture")
    }

    /// 解析为 `Value` 便于断言(仅用于能精确往返的用例)
    fn decode_value(data: &[u8]) -> Value {
        let text = decode_mysql_json_text(data).unwrap();
        serde_json::from_str(&text).expect("decoded text must be valid JSON")
    }

    /// 真实语料 1: 表 pj/pj3 的 `JSON_OBJECT('k0',0,...,'k19',19)`
    /// (MySQL 8.0.39 binlog，195 字节，20 个 int16 内联值)
    const REAL_FLAT_OBJECT: &str = "
        00 14 00 c2 00
        90 00 02 00 92 00 02 00 94 00 02 00 96 00 02 00 98 00 02 00
        9a 00 02 00 9c 00 02 00 9e 00 02 00 a0 00 02 00 a2 00 02 00
        a4 00 03 00 a7 00 03 00 aa 00 03 00 ad 00 03 00 b0 00 03 00
        b3 00 03 00 b6 00 03 00 b9 00 03 00 bc 00 03 00 bf 00 03 00
        05 00 00 05 01 00 05 02 00 05 03 00 05 04 00 05 05 00 05 06 00
        05 07 00 05 08 00 05 09 00 05 0a 00 05 0b 00 05 0c 00 05 0d 00
        05 0e 00 05 0f 00 05 10 00 05 11 00 05 12 00 05 13 00
        6b 30 6b 31 6b 32 6b 33 6b 34 6b 35 6b 36 6b 37 6b 38 6b 39
        6b 31 30 6b 31 31 6b 31 32 6b 31 33 6b 31 34 6b 31 35 6b 31 36 6b 31 37 6b 31 38 6b 31 39
    ";

    #[test]
    fn test_decode_real_flat_object() {
        let bytes = hex(REAL_FLAT_OBJECT);
        assert_eq!(195, bytes.len(), "fixture should be the real 195-byte value");

        let value = decode_value(&bytes);
        let object = value.as_object().expect("should be a JSON object");
        assert_eq!(20, object.len());
        for i in 0..20 {
            assert_eq!(Some(&json!(i)), object.get(&format!("k{}", i)), "key k{}", i);
        }
    }

    /// 真实语料 2: pj4 中 `"nested"` 的离线对象(`{"a":1}`，补上类型码 0x00)
    #[test]
    fn test_decode_real_nested_object() {
        let bytes = hex("00  01 00 0c 00  0b 00 01 00  05 01 00  61");
        assert_eq!(json!({"a": 1}), decode_value(&bytes));
    }

    /// 真实语料 3: pj4 中 `"arr"` 的离线数组(`[1,2,3]`)
    #[test]
    fn test_decode_real_array() {
        let bytes = hex("02  03 00 0d 00  05 01 00 05 02 00 05 03 00");
        assert_eq!(json!([1, 2, 3]), decode_value(&bytes));
    }

    /// 真实语料 4: pj4 中 `"name"` 的离线字符串(`"hello world"`)
    #[test]
    fn test_decode_real_string() {
        let bytes = hex("0c  0b 68 65 6c 6c 6f 20 77 6f 72 6c 64");
        assert_eq!(json!("hello world"), decode_value(&bytes));
    }

    #[test]
    fn test_decode_scalars() {
        assert_eq!(json!(null), decode_value(&hex("04 00")));
        assert_eq!(json!(true), decode_value(&hex("04 01")));
        assert_eq!(json!(false), decode_value(&hex("04 02")));

        assert_eq!(json!(-2), decode_value(&hex("05 fe ff")));
        assert_eq!(json!(65535), decode_value(&hex("06 ff ff")));
        assert_eq!(json!(-3), decode_value(&hex("07 fd ff ff ff")));
        assert_eq!(json!(4294967295u32), decode_value(&hex("08 ff ff ff ff")));

        // 真实语料: "big" = 123456789012345 (int64)
        assert_eq!(json!(123456789012345i64), decode_value(&hex("09 79 df 0d 86 48 70 00 00")));
        // 真实语料: "u" = 18446744073709551615 (uint64)
        assert_eq!(
            json!(18446744073709551615u64),
            decode_value(&hex("0a ff ff ff ff ff ff ff ff"))
        );
    }

    /// 真实语料 5: pj5 的 `JSON_OBJECT('a',-3.5,'b',12345.6789,'c',0,'d',-0.001,'e',<39,9>,'f',7)`
    /// MySQL 自身给出的文本:
    /// `{"a": -3.5, "b": 12345.6789, "c": 0, "d": -0.001, "e": 123456789012345678901234567890.123456789, "f": 7}`
    #[test]
    fn test_decode_real_opaque_decimals() {
        // "a" = -3.5 → opaque NEWDECIMAL, precision=2 scale=1
        assert_eq!("-3.5", decode_mysql_json_text(&hex("0f f6 04 02 01 7c fa")).unwrap());
        // "b" = 12345.6789 → precision=9 scale=4
        assert_eq!(
            "12345.6789",
            decode_mysql_json_text(&hex("0f f6 07 09 04 80 30 39 1a 85")).unwrap()
        );
        // "d" = -0.001 → precision=4 scale=3(部分组 + 末尾补零)
        assert_eq!("-0.001", decode_mysql_json_text(&hex("0f f6 05 04 03 7f ff fe")).unwrap());
        // "e" = 123456789012345678901234567890.123456789 → precision=39 scale=9
        // 这是 f64 无法精确表示的值，必须逐位保真
        assert_eq!(
            "123456789012345678901234567890.123456789",
            decode_mysql_json_text(&hex(
                "0f f6 14 27 09 80 7b 1b 3a 0c 14 14 9a a4 35 0d fb 38 d2 07 5b cd 15"
            ))
            .unwrap()
        );
    }

    /// 整篇真实文档(97 字节)与 MySQL 的文本输出逐字符一致
    #[test]
    fn test_decode_real_full_document_with_decimals() {
        let bytes = hex("
            00 06 00 60 00
            2e 00 01 00 2f 00 01 00 30 00 01 00 31 00 01 00 32 00 01 00 33 00 01 00
            0f 34 00 0f 3a 00 05 00 00 0f 43 00 0f 4a 00 05 07 00
            61 62 63 64 65 66
            f6 04 02 01 7c fa
            f6 07 09 04 80 30 39 1a 85
            f6 05 04 03 7f ff fe
            f6 14 27 09 80 7b 1b 3a 0c 14 14 9a a4 35 0d fb 38 d2 07 5b cd 15
        ");
        assert_eq!(97, bytes.len());

        assert_eq!(
            r#"{"a":-3.5,"b":12345.6789,"c":0,"d":-0.001,"e":123456789012345678901234567890.123456789,"f":7}"#,
            decode_mysql_json_text(&bytes).unwrap()
        );
    }

    /// MySQL 未实现的 opaque 自定义类型必须显式报错，不能静默给出错误值
    #[test]
    fn test_unknown_opaque_type_is_reported() {
        // custom type 10 = DATE(hypothetical payload)
        let err = decode_mysql_json_text(&hex("0f 0a 03 01 02 03"))
            .expect_err("unknown opaque type must not be silently decoded");
        let message = format!("{}", err);
        assert!(message.contains("opaque"), "got: {}", message);
        assert!(message.contains("10"), "should name the custom type, got: {}", message);
    }

    #[test]
    fn test_malformed_input_is_rejected() {
        assert!(decode_mysql_json_text(&[]).is_err());
        assert!(decode_mysql_json_text(&hex("7f 00")).is_err());
        // 元素个数超过剩余字节(避免巨大 count 造成越界或长时间循环)
        assert!(decode_mysql_json_text(&hex("00 ff ff 10 00")).is_err());
        // 截断的字符串
        assert!(decode_mysql_json_text(&hex("0c 10 61 62")).is_err());
        // 声明 2 个成员但缺少键条目
        assert!(decode_mysql_json_text(&hex("00 02 00 10 00 90 00 02 00")).is_err());
        // packed decimal 长度与 precision/scale 不匹配
        assert!(decode_mysql_json_text(&hex("0f f6 04 09 04 01 02 03 04")).is_err());
    }
}

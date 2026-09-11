//! `PARTIAL_UPDATE_ROWS_EVENT`(MySQL 8.0.20+，事件号 39)的解析入口。
//!
//! 该事件是 `UPDATE_ROWS_EVENT` 的扩展: 行数据在 before-image 与 after-image
//! 之间插入了 `value_options` 与 `partial_bits`，用于承载 JSON 列的增量(diff)。
//!
//! 本模块只负责"头 + 行数据定位"，行级语义见
//! [`RowParser::parse_partial_update_row_data_list`](crate::row::parser::RowParser::parse_partial_update_row_data_list)。
//!
//! 解析成功后产出与普通 UPDATE 完全一致的 [`UpdateRowsEvent`]，
//! 因此下游(pipeline / sink)无需区分事件来源。

use std::collections::HashMap;
use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;

use common::err::decode_error::ReError;

use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
use crate::row::parser::{parse_head, RowParser};
use crate::utils::read_bitmap_little_endian;

/// 解析 `PARTIAL_UPDATE_ROWS_EVENT`。
///
/// 结构与 `UPDATE_ROWS_EVENT` 一致(同一 post-header、同一 before/after 列位图)，
/// 差异仅在**行内**:
///
/// ```text
/// row = before_image || value_options(lenenc) || [partial_bits] || after_image
/// ```
pub fn parse_partial_update_rows_event(
    cursor: &mut Cursor<&[u8]>,
    header: HeaderRef,
    context: LogContextRef,
    table_map: Option<&HashMap<u64, TableMapEvent>>,
) -> Result<UpdateRowsEvent, ReError> {
    let post_header_len = context
        .borrow()
        .get_format_description()
        .get_post_header_len(header.borrow().get_event_type() as usize);

    let (table_id, flags, extra_data_len, extra_data, columns_number, version) =
        parse_head(cursor, post_header_len)?;

    let before_image = read_bitmap_little_endian(cursor, columns_number)?;
    let after_image = read_bitmap_little_endian(cursor, columns_number)?;

    // 行数据(末尾 4 字节是事件校验和，不属于行数据)
    let remaining_len = cursor.remaining();
    let mut rows_data_vec = vec![0; remaining_len - 4];
    cursor.read_exact(&mut rows_data_vec)?;
    let mut rows_data_cursor = Cursor::new(rows_data_vec.as_slice());

    let mut row_parser = RowParser::with_default_cache();

    // 注册 table map，供行解析按列类型取值
    if let Some(table_maps) = table_map {
        for (tid, tmap) in table_maps {
            row_parser.register_table_map(*tid, tmap.clone())?;
        }
    }

    let rows = row_parser.parse_partial_update_row_data_list(
        &mut rows_data_cursor,
        table_id,
        &before_image,
        &after_image,
    )?;

    let checksum = cursor.read_u32::<LittleEndian>()?;
    header.borrow_mut().update_checksum(checksum);

    Ok(UpdateRowsEvent::new(
        Header::copy(header),
        table_id,
        flags,
        extra_data_len,
        extra_data,
        columns_number,
        before_image,
        after_image,
        rows,
        version,
    ))
}

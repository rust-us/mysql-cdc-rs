use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::rc::Rc;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::ExtraData;
use crate::row::row_data::{UpdateRowData};
use crate::row::row_parser::{parse_head, parse_update_row_data_list};
use crate::row::rows::RowEventVersion;
use crate::utils::read_bitmap_little_endian;

/// Represents one or many updated rows in row based replication.
/// Includes versions before and after update.
/// <a href="https://mariadb.com/kb/en/library/rows_event_v1/">See more</a>
#[derive(Debug, Serialize, Clone)]
pub struct UpdateRowsEvent {
    header: Header,

    /// Gets id of the table where rows were updated
    pub table_id: u64,

    flags: u16, // Flags,

    extra_data_len: u16,
    extra_data: Vec<ExtraData>,

    /// Gets number of columns in the table, Bitmap denoting columns available.
    pub columns_number: usize,

    /// Gets bitmap of columns present in row event before update. See binlog_row_image parameter.
    // before_image_bits: Vec<u8>,
    pub before_image_bits: Vec<bool>,

    /// Gets bitmap of columns present in row event after update. See binlog_row_image parameter.
    // after_image_bits: Vec<u8>,
    pub after_image_bits: Vec<bool>,

    /// 存储修改前和修改后的数据
    pub rows: Vec<UpdateRowData>,

    row_version: RowEventVersion,
}

impl UpdateRowsEvent {

    pub fn new(header: Header,
               table_id: u64,  flags: u16, extra_data_len: u16, extra_data: Vec<ExtraData>,
               columns_number: usize, before_image_bits: Vec<bool>, after_image_bits: Vec<bool>,
               rows: Vec<UpdateRowData>, row_version: RowEventVersion) -> Self {
        UpdateRowsEvent {
            header,
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            before_image_bits,
            after_image_bits,
            rows,
            row_version,
        }
    }

    /// Supports all versions of MariaDB and MySQL 5.5+ (V1 and V2 row events).
    pub fn parse(
        cursor: &mut Cursor<&[u8]>,
        table_map: &HashMap<u64, TableMapEvent>,
        header: &Header, context: Rc<RefCell<LogContext>>) -> Result<Self, ReError> {

        let _context = context.borrow();
        let post_header_len = _context.get_format_description().get_post_header_len(header.get_event_type() as usize);

        let (table_id, flags, extra_data_len, extra_data, columns_number, version) =
                    parse_head(cursor, post_header_len)?;

        let before_image = read_bitmap_little_endian(cursor, columns_number)?;
        let after_image = read_bitmap_little_endian(cursor, columns_number)?;

        // rows_data_cursor
        let _remaining_len = cursor.remaining();
        let mut _rows_data_vec = vec![0; (_remaining_len - 4)];
        cursor.read_exact(&mut _rows_data_vec)?;

        let mut _rows_data_cursor = Cursor::new(_rows_data_vec.as_slice());
        let rows = parse_update_row_data_list(
            &mut _rows_data_cursor,
            table_map,
            table_id,
            &before_image,
            &after_image,
        )?;

        let checksum = cursor.read_u32::<LittleEndian>()?;

        let e = UpdateRowsEvent::new(
            Header::copy_and_get(header, checksum, vec![]),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            before_image,
            after_image,
            rows,
            version,
        );

        Ok(e)
    }
}

impl LogEvent for UpdateRowsEvent {

}
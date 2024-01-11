use crate::events::event::Event;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::log_event::LogEvent;
use crate::events::protocol::rotate_event::RotateEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::RowData;
use crate::row::row_parser::{parse_head, parse_row_data_list};
use crate::row::rows::RowEventVersion;
use crate::utils::{read_bitmap_little_endian, read_bitmap_little_endian_bits};
use crate::ExtraData;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use common::err::DecodeError::ReError;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::rc::Rc;
use crate::events::event_raw::HeaderRef;

/// Represents one or many deleted rows in row based replication.
/// <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/classDelete__rows__log__event.html">See more</a>
/// <a href="https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-row-events.html">See more</a>
#[derive(Debug, Serialize, Clone)]
pub struct DeleteRowsEvent {
    pub header: Header,

    /// Gets id of the table where rows were updated
    pub table_id: u64,

    pub flags: u16, // Flags,

    pub extra_data_len: u16,
    pub extra_data: Vec<ExtraData>,

    /// Gets number of columns in the table, Bitmap denoting columns available.
    pub columns_number: usize,

    /// Gets bitmap of columns present in row event. See binlog_row_image parameter.
    pub deleted_image_bits: Vec<bool>,
    // pub deleted_image_bits: Vec<u8>,
    /// 存储删除前的数据
    pub rows: Vec<RowData>,

    pub row_version: RowEventVersion,
}

impl DeleteRowsEvent {
    pub fn new(
        header: Header,
        table_id: u64,
        flags: u16,
        extra_data_len: u16,
        extra_data: Vec<ExtraData>,
        columns_number: usize,
        deleted_image_bits: Vec<bool>,
        rows: Vec<RowData>,
        row_version: RowEventVersion,
    ) -> Self {
        DeleteRowsEvent {
            header,
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            deleted_image_bits,
            rows,
            row_version,
        }
    }

}

impl LogEvent for DeleteRowsEvent {
    fn get_type_name(&self) -> String {
        "DeleteRowsEvent".to_string()
    }


    /// Supports all versions of MariaDB and MySQL 5.5+ (V1 and V2 row events).
    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
    ) -> Result<DeleteRowsEvent, ReError> {
        let _context = context.borrow();
        let post_header_len = _context
            .get_format_description()
            .get_post_header_len(header.borrow_mut().get_event_type() as usize);

        let (table_id, flags, extra_data_len, extra_data, columns_number, version) =
            parse_head(cursor, post_header_len)?;

        // after_image
        let deleted_image_bits = read_bitmap_little_endian(cursor, columns_number)?;

        // rows_data_cursor
        let _remaining_len = cursor.remaining();
        let mut _rows_data_vec = vec![0; (_remaining_len - 4)];
        cursor.read_exact(&mut _rows_data_vec)?;
        let mut _rows_data_cursor = Cursor::new(_rows_data_vec.as_slice());
        let rows = parse_row_data_list(
            &mut _rows_data_cursor,
            table_map.unwrap(),
            table_id,
            &deleted_image_bits,
        )?;

        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        Ok(DeleteRowsEvent {
            header: Header::copy(header.clone()),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            deleted_image_bits,
            rows,
            row_version: version,
        })
    }
}

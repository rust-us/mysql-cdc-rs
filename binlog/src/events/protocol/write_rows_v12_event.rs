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
use crate::row::row_data::RowData;
use crate::row::row_parser::{parse_row_data_list, parse_head, TABLE_MAP_NOT_FOUND};
use crate::row::rows;
use crate::row::rows::RowEventVersion;
use crate::utils::{read_bitmap_little_endian, read_bitmap_little_endian_bits};

///                          Binary_log_event
///                                   ^
///                                   |
///                                   |
///                                   |
///                 Log_event   B_l:Rows_event
///                      ^            /\
///                      |           /  \
///                      |   <<vir>>/    \ <<vir>>
///                      |         /      \
///                      |        /        \
///                      |       /          \
///                   Rows_log_event    B_l:Write_rows_event
///                              \          /
///                               \        /
///                                \      /
///                                 \    /
///                                  \  /
///                                   \/
///                         Write_rows_log_event
///
///   B_l: Namespace Binary_log
#[derive(Debug, Serialize, Clone)]
pub struct WriteRowsEvent {
    header: Header,

    // Post-Header for Rows_event Start
    /// table_id take 6 bytes in buffer.  The number that identifies the table
    pub table_id: u64,

    // 2 byte bitfield. Reserved for future use; currently always 0.
    // eg flags:
    //     end_of_stmt: true
    //     foreign_key_checks: true
    //     unique_key_checks: true
    //     has_columns: true
    flags: u16,
    // Post-Header for Rows_event END

    // event-body部分 for Rows_event Start
    extra_data_len: u16,
    extra_data: Vec<rows::ExtraData>,

    /// Gets number of columns in the table, Bitmap denoting columns available.
    pub columns_number: usize,

    /// Gets bitmap of columns present in row event. See binlog_row_image parameter.
    pub columns_present: Vec<bool>,
    // pub columns_present: Vec<u8>,

    /// 存储插入的数据
    pub rows: Vec<RowData>,

    // event-body部分 for Rows_event END

    row_version: RowEventVersion,
}

impl WriteRowsEvent {

    pub fn new(header: Header,
               table_id: u64,  flags: u16, extra_data_len: u16, extra_data: Vec<rows::ExtraData>,
               columns_number: usize, columns_present: Vec<bool>,
               rows: Vec<RowData>, row_version: RowEventVersion) -> Self {
        WriteRowsEvent {
            header,
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            columns_present,
            rows,
            row_version,
        }
    }

    ///
    ///
    /// # Arguments
    ///
    /// * `input`:
    /// * `header`:
    /// * `context`:
    /// * `row_event_version`: 事件版本，1 是指WriteRowsEventV1。 2是指MySqlWriteRowsEventV2
    ///
    /// returns: Result<(&[u8], WriteRowsV2Event), Err<Error<&[u8]>>>
    pub fn parse<'a>(cursor: &mut Cursor<&[u8]>,
                     table_map: &HashMap<u64, TableMapEvent>,
                     header: &Header, context: Rc<RefCell<LogContext>>)
        -> Result<Self, ReError> {

        let _context = context.borrow();
        let common_header_len = _context.get_format_description().common_header_len;
        let query_post_header_len = _context.get_format_description().get_post_header_len(header.get_event_type() as usize);

        let (table_id, flags, extra_data_len, extra_data, columns_number, version) =
                    parse_head(cursor, query_post_header_len)?;

        let columns_present = read_bitmap_little_endian(cursor, columns_number)?;

        // rows_data_cursor
        let remaining_len = cursor.remaining();
        let mut rows_data_vec = vec![0; (remaining_len - 4)];
        cursor.read_exact(&mut rows_data_vec)?;

        let mut rows_data_cursor = Cursor::new(rows_data_vec.as_slice());
        let rows = parse_row_data_list(&mut rows_data_cursor, table_map, table_id, &columns_present);

        let checksum = cursor.read_u32::<LittleEndian>()?;

        let e = WriteRowsEvent::new(
            Header::copy_and_get(header, checksum, vec![]),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            columns_present,
            rows.unwrap(),
            version
        );

        Ok(e)
    }
}

impl LogEvent for WriteRowsEvent {

}
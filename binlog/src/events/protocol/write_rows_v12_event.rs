use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::RowData;
use crate::row::row_parser::{parse_head, parse_row_data_list};
use crate::row::rows;
use crate::row::rows::{RowEventVersion, STMT_END_F};
use crate::utils::{read_bitmap_little_endian};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use common::err::DecodeError::{Needed, ReError};
use serde::Serialize;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use dashmap::mapref::one::Ref;
use crate::column::column_type::ColumnType;
use crate::events::declare::rows_log_event::RowsLogEvent;
use crate::events::event_raw::HeaderRef;

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

    /// event-body部分 for Rows_event END
    row_version: RowEventVersion,

    /// The table the rows belong to
    table: Option<TableMapEvent>,
    json_column_count: u32,
}

impl WriteRowsEvent {
    pub fn new(
        header: Header,
        table_id: u64,
        flags: u16,
        extra_data_len: u16,
        extra_data: Vec<rows::ExtraData>,
        columns_number: usize,
        columns_present: Vec<bool>,
        rows: Vec<RowData>,
        row_version: RowEventVersion,
    ) -> Self {
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
            table: None,
            json_column_count: 0,
        }
    }
}

impl RowsLogEvent for WriteRowsEvent {
    fn fill_assembly_table(&mut self, context: LogContextRef) -> Result<bool, ReError> {
        let table_id = self.table_id;

        {
            let mut context_borrow = context.borrow_mut();

            let table: Option<Ref<u64, TableMapEvent>>  = context_borrow.get_table(&table_id);

            if table.is_none() {
                return Err(ReError::Incomplete(Needed::InvalidData(
                    format!("not found tableId error, tableId: {}", table_id)
                )));
            }

            let new_table = TableMapEvent::copy(table.unwrap().value());
            self.table = Some(new_table.clone());

            // end of statement check
            if (self.flags & STMT_END_F as u16) != 0 {
                // Now is safe to clear ignored map (clear_tables will also delete original table map events stored in the map).
                context_borrow.clear_all_table();
            }

            let mut json_column_count = 0;
            let column_count = new_table.get_columns_number();
            let column_metadata_type = new_table.get_column_metadata_type();
            assert_eq!(column_count as usize, column_metadata_type.len());

            for clolumn_type in column_metadata_type {
                if clolumn_type == ColumnType::Json {
                    json_column_count += 1;
                }
            }
            self.json_column_count = json_column_count;
        }

        Ok(true)
    }
}

impl LogEvent for WriteRowsEvent {
    fn get_type_name(&self) -> String {
        "WriteRowsEvent".to_string()
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
    ) -> Result<Self, ReError> {
        let _context = context.borrow();
        let common_header_len = _context.get_format_description().common_header_len;
        let query_post_header_len = _context
            .get_format_description()
            .get_post_header_len(header.borrow_mut().get_event_type() as usize);

        let (table_id, flags, extra_data_len, extra_data, columns_number, version) =
            parse_head(cursor, query_post_header_len)?;

        let columns_present = read_bitmap_little_endian(cursor, columns_number)?;

        // rows_data_cursor
        let remaining_len = cursor.remaining();
        let mut rows_data_vec = vec![0; (remaining_len - 4)];
        cursor.read_exact(&mut rows_data_vec)?;

        let mut rows_data_cursor = Cursor::new(rows_data_vec.as_slice());
        let rows =
            parse_row_data_list(&mut rows_data_cursor, table_map.unwrap(), table_id, &columns_present);

        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        let e = WriteRowsEvent::new(
            Header::copy(header.clone()),
            table_id,
            flags,
            extra_data_len,
            extra_data,
            columns_number,
            columns_present,
            rows.unwrap(),
            version,
        );

        Ok(e)
    }
}

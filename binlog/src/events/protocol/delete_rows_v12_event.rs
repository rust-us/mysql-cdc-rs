use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::RowData;
use crate::row::row_parser::{parse_head, parse_row_data_list};
use crate::row::rows::{RowEventVersion, STMT_END_F};
use crate::utils::{read_bitmap_little_endian};
use crate::ExtraData;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use common::err::decode_error::{Needed, ReError};
use serde::Serialize;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use dashmap::mapref::one::Ref;
use common::binlog::column::column_type::SrcColumnType;
use crate::events::declare::rows_log_event::RowsLogEvent;
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

    /// The table the rows belong to
    table: Option<TableMapEvent>,
    json_column_count: u32,
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
            table: None,
            json_column_count: 0,
        }
    }

    pub fn get_rows(&self) -> &[RowData] {
        self.rows.as_slice()
    }
}

impl RowsLogEvent for DeleteRowsEvent {
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
                if clolumn_type == SrcColumnType::Json {
                    json_column_count += 1;
                }
            }
            self.json_column_count = json_column_count;
        }

        Ok(true)
    }

    fn get_table_map_event(&self) -> Option<&TableMapEvent> {
        self.table.as_ref()
    }

    fn get_header(&self) -> Header {
        self.header.clone()
    }
}

impl LogEvent for DeleteRowsEvent {
    fn get_type_name(&self) -> String {
        "DeleteRowsEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
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
            table: None,
            json_column_count: 0,
        })
    }
}

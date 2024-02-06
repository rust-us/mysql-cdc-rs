use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::UpdateRowData;
use crate::row::row_parser::{parse_head, parse_update_row_data_list};
use crate::row::rows::{RowEventVersion, STMT_END_F};
use crate::utils::read_bitmap_little_endian;
use crate::ExtraData;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use common::err::decode_error::{Needed, ReError};
use serde::Serialize;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use dashmap::mapref::one::Ref;
use common::binlog::column::column_type::SrcColumnType;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::declare::rows_log_event::RowsLogEvent;
use crate::events::event_raw::HeaderRef;

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

    /// The table the rows belong to
    table: Option<TableMapEvent>,
    json_column_count: u32,
}

impl UpdateRowsEvent {
    pub fn new(
        header: Header,
        table_id: u64,
        flags: u16,
        extra_data_len: u16,
        extra_data: Vec<ExtraData>,
        columns_number: usize,
        before_image_bits: Vec<bool>,
        after_image_bits: Vec<bool>,
        rows: Vec<UpdateRowData>,
        row_version: RowEventVersion,
    ) -> Self {
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
            table: None,
            json_column_count: 0,
        }
    }

    pub fn get_table_id(&self) -> u64 {
        self.table_id
    }

    pub fn get_columns_number(&self) -> usize {
        self.columns_number
    }

    pub fn get_rows(&self) -> &[UpdateRowData] {
        self.rows.as_slice()
    }
}

impl RowsLogEvent for UpdateRowsEvent {
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

impl LogEvent for UpdateRowsEvent {
    fn get_type_name(&self) -> String {
        "UpdateRowsEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<Self, ReError> {
        let _context = context.borrow();
        let post_header_len = _context
            .get_format_description()
            .get_post_header_len(header.borrow_mut().get_event_type() as usize);

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
            table_map.unwrap(),
            table_id,
            &before_image,
            &after_image,
        )?;

        let checksum = cursor.read_u32::<LittleEndian>()?;

        header.borrow_mut().update_checksum(checksum);
        let e = UpdateRowsEvent::new(
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
        );

        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use common::binlog::column::column_value::SrcColumnValue::{Blob, Decimal, Double, Float, Int, String};
    use crate::events::event_header::Header;
    use crate::events::protocol::table_map_event::TableMapEvent;
    use crate::events::protocol::update_rows_v12_event::UpdateRowsEvent;
    use crate::row::row_data::{RowData, UpdateRowData};
    use crate::row::rows::RowEventVersion;

    #[test]
    fn test_get_rows() {
        assert_eq!(1, 1);

        let mut rows: Vec<UpdateRowData> = Vec::new();


        // values
        let abc = "abc".to_string();
        let xd = "xd".to_string();
        let abc_bytes = vec![97, 98, 99];
        let xd_bytes = vec![120, 100];
;
        let row = UpdateRowData::new(
            RowData {
                cells: vec![
                    Some(Int(1)),
                    Some(String(abc.clone())),
                    Some(String(abc.clone())),
                    Some(Blob(abc_bytes.clone())),
                    Some(Blob(abc_bytes.clone())),
                    Some(Blob(abc_bytes.clone())),
                    Some(Float(1.0)),
                    Some(Double(2.0)),
                    Some(Decimal("3.0000".to_string())), // NewDecimal(vec![128, 0, 3, 0, 0])
                ],
            },
            RowData {
                cells: vec![
                    Some(Int(1)),
                    Some(String(xd.clone())),
                    Some(String(xd.clone())),
                    Some(Blob(xd_bytes.clone())),
                    Some(Blob(xd_bytes.clone())),
                    Some(Blob(xd_bytes.clone())),
                    Some(Float(4.0)),
                    Some(Double(4.0)),
                    Some(Decimal("4.0000".to_string())), //  NewDecimal(vec![128, 0, 4, 0, 0])
                ],
            });
        rows.push(row);

        let update_event = UpdateRowsEvent {
            header: Header::default(),
            table_id: 1,
            flags: 1,
            extra_data_len: 1,
            extra_data: vec![],
            columns_number: 1,
            before_image_bits: vec![],
            after_image_bits: vec![],
            rows,
            row_version: RowEventVersion::V1,
            table: Some(TableMapEvent::default()),
            json_column_count: 0,
        };

        let get_rows = &update_event.get_rows();
        for i in 0..get_rows.len() {
            println!("{:?}", get_rows[i]);
        }
        assert_eq!(&update_event.get_rows().len(), &1);
    }
}

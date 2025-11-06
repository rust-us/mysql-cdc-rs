use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::declare::log_event::LogEvent;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::row_data::UpdateRowData;
use crate::row::parser::{parse_head, RowParser};
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

/// Statistics for update event analysis
#[derive(Debug, Clone)]
pub struct UpdateEventStatistics {
    pub total_rows: usize,
    pub total_columns: usize,
    pub total_changed_fields: usize,
    pub partial_updates: usize,
    pub total_memory_usage: usize,
    pub difference_memory_overhead: usize,
    pub average_change_percentage: f64,
    pub memory_overhead_percentage: f64,
}

impl UpdateEventStatistics {
    pub fn new() -> Self {
        Self {
            total_rows: 0,
            total_columns: 0,
            total_changed_fields: 0,
            partial_updates: 0,
            total_memory_usage: 0,
            difference_memory_overhead: 0,
            average_change_percentage: 0.0,
            memory_overhead_percentage: 0.0,
        }
    }

    pub fn finalize(&mut self) {
        if self.total_columns > 0 {
            self.average_change_percentage = 
                (self.total_changed_fields as f64 / self.total_columns as f64) * 100.0;
        }
        
        if self.total_memory_usage > 0 {
            self.memory_overhead_percentage = 
                (self.difference_memory_overhead as f64 / self.total_memory_usage as f64) * 100.0;
        }
    }

    pub fn partial_update_ratio(&self) -> f64 {
        if self.total_rows > 0 {
            (self.partial_updates as f64 / self.total_rows as f64) * 100.0
        } else {
            0.0
        }
    }
}

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

    /// Get rows with mutable access for difference analysis
    pub fn get_rows_mut(&mut self) -> &mut [UpdateRowData] {
        &mut self.rows
    }

    /// Get only rows that have actual changes (for sparse update optimization)
    pub fn get_changed_rows(&self) -> Vec<&UpdateRowData> {
        self.rows
            .iter()
            .filter(|row| {
                if let Some(diff) = row.get_difference_readonly() {
                    diff.changed_count > 0
                } else {
                    // If no difference computed, assume there are changes
                    true
                }
            })
            .collect()
    }

    /// Get only rows that have actual changes (mutable version for analysis)
    pub fn get_changed_rows_mut(&mut self) -> Vec<&mut UpdateRowData> {
        // First, compute differences for all rows that don't have them
        for row in &mut self.rows {
            if row.get_difference_readonly().is_none() && row.enable_difference_detection {
                row.compute_difference();
            }
        }
        
        // Then filter for rows with changes
        self.rows
            .iter_mut()
            .filter(|row| {
                if let Some(diff) = row.get_difference_readonly() {
                    diff.changed_count > 0
                } else {
                    // If no difference computed, assume there are changes
                    true
                }
            })
            .collect()
    }

    /// Get update statistics for this event
    pub fn get_update_statistics(&mut self) -> UpdateEventStatistics {
        let mut stats = UpdateEventStatistics::new();
        
        for row in &mut self.rows {
            let diff = row.get_difference();
            stats.total_rows += 1;
            stats.total_columns += diff.total_columns;
            stats.total_changed_fields += diff.changed_count;
            
            if diff.is_partial_update() {
                stats.partial_updates += 1;
            }
            
            let memory_stats = row.get_memory_stats();
            stats.total_memory_usage += memory_stats.total_size;
            stats.difference_memory_overhead += memory_stats.difference_size;
        }
        
        stats.finalize();
        stats
    }

    /// Convert to incremental updates for memory optimization
    pub fn to_incremental_updates(&mut self) -> Vec<crate::row::row_data::IncrementalUpdate> {
        self.rows
            .iter_mut()
            .map(|row| row.to_incremental_update())
            .collect()
    }

    /// Check if this event contains mostly sparse updates
    pub fn is_sparse_update_event(&mut self, threshold_percentage: f64) -> bool {
        if self.rows.is_empty() {
            return false;
        }
        
        // First, compute differences for all rows that don't have them
        for row in &mut self.rows {
            if row.get_difference_readonly().is_none() && row.enable_difference_detection {
                row.compute_difference();
            }
        }
        
        let sparse_count = self.rows
            .iter()
            .filter(|row| {
                if let Some(diff) = row.get_difference_readonly() {
                    diff.change_percentage() < threshold_percentage
                } else {
                    false // If no difference computed, not considered sparse
                }
            })
            .count();
            
        (sparse_count as f64 / self.rows.len() as f64) * 100.0 >= 50.0 // 50% of rows are sparse
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
        
        // Create a row parser with default cache size
        let mut row_parser = RowParser::with_default_cache();
        
        // Register table maps from the provided table_map
        if let Some(table_maps) = table_map {
            for (tid, tmap) in table_maps {
                row_parser.register_table_map(*tid, tmap.clone())?;
            }
        }
        
        // Use enhanced parsing with difference detection enabled by default
        let rows = row_parser.parse_update_row_data_list_enhanced(
            &mut _rows_data_cursor,
            table_id,
            &before_image,
            &after_image,
            true, // Enable difference detection
            None, // No partial column filtering
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

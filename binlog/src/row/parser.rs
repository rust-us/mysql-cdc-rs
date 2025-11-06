use std::collections::{HashMap, VecDeque};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};
use std::sync::{Arc, RwLock};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use tracing::error;
use common::binlog::column::column_type::SrcColumnType;
use common::binlog::column::column_value::SrcColumnValue;
use common::err::decode_error::ReError;
use crate::utils::{parse_bit, parse_blob, parse_date, parse_date_time, parse_date_time2, parse_string, parse_time, parse_time2, parse_timestamp, parse_timestamp2, parse_year};
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::{ExtraData, ExtraDataFormat, Flags, Payload};
use crate::events::declare::log_event::EXTRA_ROW_INFO_HDR_BYTES;
use crate::events::protocol::format_description_log_event::ROWS_HEADER_LEN_V2;
use crate::row::actual_string_type::get_actual_string_type;
use crate::row::decimal::parse_decimal;
use crate::row::row_data::{RowData, UpdateRowData};
use crate::row::rows::{ExtraDataType, RowEventVersion};
use crate::row::event_handler::RowEventHandlerRegistry;
use crate::row::performance::{OptimizedRowParser, RowDataPool, RowParsingStats, ZeroCopyBitmap, read_bitmap_zero_copy, count_set_bits_optimized};
use crate::row::monitoring::{RowParsingMonitor, MonitoringConfig};
use crate::utils::{read_bitmap_little_endian, read_len_enc_num, read_string};

pub const TABLE_MAP_NOT_FOUND: &str =
    "No preceding TableMapEvent event was found for the row event. \
You possibly started replication in the middle of logical event group.";

/// Thread-safe local table mapping cache
#[derive(Debug, Clone)]
pub struct TableMapCache {
    cache: Arc<RwLock<HashMap<u64, TableMapEvent>>>,
    insertion_order: Arc<RwLock<VecDeque<u64>>>,
    max_size: usize,
}

impl TableMapCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            insertion_order: Arc::new(RwLock::new(VecDeque::new())),
            max_size,
        }
    }

    pub fn insert(&self, table_id: u64, table_map: TableMapEvent) -> Result<(), ReError> {
        let mut cache = self.cache.write()
            .map_err(|_| ReError::String("Failed to acquire write lock on table map cache".to_string()))?;
        let mut order = self.insertion_order.write()
            .map_err(|_| ReError::String("Failed to acquire write lock on insertion order".to_string()))?;
        
        // If key already exists, remove it from order first
        if cache.contains_key(&table_id) {
            order.retain(|&x| x != table_id);
        }
        
        // Simple LRU: if cache is full, remove oldest entry
        if cache.len() >= self.max_size && !cache.contains_key(&table_id) {
            if let Some(oldest_key) = order.pop_front() {
                cache.remove(&oldest_key);
            }
        }
        
        cache.insert(table_id, table_map);
        order.push_back(table_id);
        Ok(())
    }

    pub fn get(&self, table_id: u64) -> Result<Option<TableMapEvent>, ReError> {
        let cache = self.cache.read()
            .map_err(|_| ReError::String("Failed to acquire read lock on table map cache".to_string()))?;
        Ok(cache.get(&table_id).cloned())
    }

    pub fn get_with_stats(&self, table_id: u64) -> Result<(Option<TableMapEvent>, bool), ReError> {
        let cache = self.cache.read()
            .map_err(|_| ReError::String("Failed to acquire read lock on table map cache".to_string()))?;
        let result = cache.get(&table_id).cloned();
        let is_hit = result.is_some();
        Ok((result, is_hit))
    }

    pub fn contains(&self, table_id: u64) -> Result<bool, ReError> {
        let cache = self.cache.read()
            .map_err(|_| ReError::String("Failed to acquire read lock on table map cache".to_string()))?;
        Ok(cache.contains_key(&table_id))
    }

    pub fn clear(&self) -> Result<(), ReError> {
        let mut cache = self.cache.write()
            .map_err(|_| ReError::String("Failed to acquire write lock on table map cache".to_string()))?;
        let mut order = self.insertion_order.write()
            .map_err(|_| ReError::String("Failed to acquire write lock on insertion order".to_string()))?;
        cache.clear();
        order.clear();
        Ok(())
    }

    pub fn size(&self) -> Result<usize, ReError> {
        let cache = self.cache.read()
            .map_err(|_| ReError::String("Failed to acquire read lock on table map cache".to_string()))?;
        Ok(cache.len())
    }
}

/// Object-oriented row parser with thread-safe local table mapping
#[derive(Debug)]
pub struct RowParser {
    table_cache: TableMapCache,
    event_handlers: RowEventHandlerRegistry,
    optimized_parser: OptimizedRowParser,
    row_pool: RowDataPool,
    stats: RowParsingStats,
    monitor: RowParsingMonitor,
    enable_optimizations: bool,
}

impl RowParser {
    pub fn new(cache_size: usize) -> Self {
        Self {
            table_cache: TableMapCache::new(cache_size),
            event_handlers: RowEventHandlerRegistry::new(),
            optimized_parser: OptimizedRowParser::new(),
            row_pool: RowDataPool::new(100),
            stats: RowParsingStats::new(),
            monitor: RowParsingMonitor::new(),
            enable_optimizations: true,
        }
    }

    pub fn with_default_cache() -> Self {
        Self::new(1000) // Default cache size
    }
    
    /// Create a new parser with optimizations disabled (for compatibility)
    pub fn new_legacy(cache_size: usize) -> Self {
        let mut parser = Self::new(cache_size);
        parser.enable_optimizations = false;
        parser
    }

    /// Create a new parser with custom monitoring configuration
    pub fn new_with_monitoring(cache_size: usize, monitoring_config: MonitoringConfig) -> Self {
        Self {
            table_cache: TableMapCache::new(cache_size),
            event_handlers: RowEventHandlerRegistry::new(),
            optimized_parser: OptimizedRowParser::new(),
            row_pool: RowDataPool::new(100),
            stats: RowParsingStats::new(),
            monitor: RowParsingMonitor::with_config(monitoring_config),
            enable_optimizations: true,
        }
    }
    
    /// Enable or disable performance optimizations
    pub fn set_optimizations_enabled(&mut self, enabled: bool) {
        self.enable_optimizations = enabled;
    }
    
    /// Get a mutable reference to the event handler registry
    pub fn event_handlers_mut(&mut self) -> &mut RowEventHandlerRegistry {
        &mut self.event_handlers
    }
    
    /// Get a reference to the event handler registry
    pub fn event_handlers(&self) -> &RowEventHandlerRegistry {
        &self.event_handlers
    }
    
    /// Get parsing statistics
    pub fn get_stats(&self) -> &RowParsingStats {
        &self.stats
    }
    
    /// Reset parsing statistics
    pub fn reset_stats(&mut self) {
        self.stats.reset();
    }
    
    /// Clear the row data pool
    pub fn clear_row_pool(&mut self) {
        self.row_pool.clear();
    }

    /// Get the monitoring system
    pub fn get_monitor(&self) -> &RowParsingMonitor {
        &self.monitor
    }

    /// Get mutable access to the monitoring system
    pub fn get_monitor_mut(&mut self) -> &mut RowParsingMonitor {
        &mut self.monitor
    }

    /// Get comprehensive statistics report
    pub fn get_statistics_report(&self) -> crate::row::monitoring::StatisticsReport {
        self.monitor.get_statistics_report()
    }

    /// Reset monitoring statistics
    pub fn reset_monitoring_statistics(&mut self) {
        self.monitor.reset_statistics();
    }

    /// Update monitoring configuration
    pub fn update_monitoring_config(&mut self, config: MonitoringConfig) {
        self.monitor.update_config(config);
    }

    /// Generate a human-readable monitoring summary
    pub fn generate_monitoring_summary(&self) -> String {
        self.monitor.get_statistics_report().generate_summary()
    }

    /// Register a table map event in the local cache
    pub fn register_table_map(&self, table_id: u64, table_map: TableMapEvent) -> Result<(), ReError> {
        self.table_cache.insert(table_id, table_map)
    }

    /// Get table map from local cache
    pub fn get_table_map(&self, table_id: u64) -> Result<Option<TableMapEvent>, ReError> {
        self.table_cache.get(table_id)
    }

    /// Get table map from local cache with cache statistics tracking
    pub fn get_table_map_with_stats(&mut self, table_id: u64) -> Result<Option<TableMapEvent>, ReError> {
        let (result, is_hit) = self.table_cache.get_with_stats(table_id)?;
        if is_hit {
            self.monitor.record_cache_hit();
        } else {
            self.monitor.record_cache_miss();
        }
        Ok(result)
    }

    /// Parse row data list for INSERT/DELETE events
    pub fn parse_row_data_list(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_id: u64,
        columns_present: &[bool],
    ) -> Result<Vec<RowData>, ReError> {
        let table_map = self.get_table_map(table_id)?
            .ok_or_else(|| ReError::String(TABLE_MAP_NOT_FOUND.to_string()))?;

        // Notify handlers that table processing is starting
        if let Err(e) = self.event_handlers.process_table_start(&table_map) {
            self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
            return Err(e);
        }

        let cells_included = get_bits_number(columns_present);
        let mut rows = Vec::new();

        while cursor.has_remaining() {
            let row_result = self.parse_row(cursor, &table_map, columns_present, cells_included);

            match row_result {
                Ok(row) => rows.push(row),
                Err(ReError::IoError(io_error)) => {
                    // Handle end of file gracefully
                    if io_error.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        error!("IO error during row parsing: {:?}", io_error);
                        self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                        return Err(ReError::IoError(io_error));
                    }
                }
                Err(error) => {
                    error!("Error parsing row: {:?}", error);
                    // Notify handlers of the error
                    self.event_handlers.on_error(&table_map, &error).ok();
                    self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                    return Err(error);
                }
            }
        }

        // Notify handlers that table processing is ending
        self.event_handlers.process_table_end(&table_map)?;

        Ok(rows)
    }

    /// Parse update row data list for UPDATE events
    pub fn parse_update_row_data_list(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_id: u64,
        before_image: &[bool],
        after_image: &[bool],
    ) -> Result<Vec<UpdateRowData>, ReError> {
        let table_map = self.get_table_map(table_id)?
            .ok_or_else(|| ReError::String(TABLE_MAP_NOT_FOUND.to_string()))?;

        // Notify handlers that table processing is starting
        if let Err(e) = self.event_handlers.process_table_start(&table_map) {
            self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
            return Err(e);
        }

        let cells_included_before_update = get_bits_number(before_image);
        let cells_included_after_update = get_bits_number(after_image);
        let mut rows = Vec::new();

        while cursor.has_remaining() {
            let row_before_update = self.parse_row(
                cursor,
                &table_map,
                before_image,
                cells_included_before_update,
            )?;

            let row_after_update = self.parse_row(
                cursor,
                &table_map,
                after_image,
                cells_included_after_update,
            )?;

            // Notify handlers of the update event
            if let Err(e) = self.event_handlers.process_update(&table_map, &row_before_update, &row_after_update) {
                self.event_handlers.on_error(&table_map, &e).ok();
                self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                self.monitor.record_error(&e, Some(&table_map), false);
                return Err(e);
            }

            // Create UpdateRowData with difference detection enabled for better performance analysis
            let update_row = UpdateRowData::new_with_difference_detection(row_before_update, row_after_update);
            rows.push(update_row);
        }

        // Notify handlers that table processing is ending
        self.event_handlers.process_table_end(&table_map)?;

        // Record the update operation in monitoring
        self.monitor.record_update_operation(&table_map, &rows);

        Ok(rows)
    }

    /// Parse update row data list with enhanced difference detection and partial column support
    pub fn parse_update_row_data_list_enhanced(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_id: u64,
        before_image: &[bool],
        after_image: &[bool],
        enable_difference_detection: bool,
        partial_columns: Option<&[usize]>,
    ) -> Result<Vec<UpdateRowData>, ReError> {
        let table_map = self.get_table_map(table_id)?
            .ok_or_else(|| ReError::String(TABLE_MAP_NOT_FOUND.to_string()))?;

        // Notify handlers that table processing is starting
        if let Err(e) = self.event_handlers.process_table_start(&table_map) {
            self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
            return Err(e);
        }

        let cells_included_before_update = get_bits_number(before_image);
        let cells_included_after_update = get_bits_number(after_image);
        let mut rows = Vec::new();

        while cursor.has_remaining() {
            let start_time = std::time::Instant::now();
            
            let row_before_update = self.parse_row(
                cursor,
                &table_map,
                before_image,
                cells_included_before_update,
            )?;

            let row_after_update = self.parse_row(
                cursor,
                &table_map,
                after_image,
                cells_included_after_update,
            )?;

            // Create UpdateRowData based on configuration
            let update_row = if let Some(columns) = partial_columns {
                // Partial column update
                UpdateRowData::new_partial_update(row_before_update, row_after_update, columns)
            } else if enable_difference_detection {
                // Full difference detection
                UpdateRowData::new_with_difference_detection(row_before_update, row_after_update)
            } else {
                // Basic update without difference detection
                UpdateRowData::new(row_before_update, row_after_update)
            };

            // Notify handlers of the update event
            if let Err(e) = self.event_handlers.process_update(&table_map, &update_row.before_update, &update_row.after_update) {
                self.event_handlers.on_error(&table_map, &e).ok();
                self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                return Err(e);
            }

            rows.push(update_row);
            
            // Update performance statistics
            let parse_time = start_time.elapsed().as_nanos() as u64;
            self.stats.add_row(0, parse_time); // We don't track bytes for update rows separately
        }

        // Notify handlers that table processing is ending
        self.event_handlers.process_table_end(&table_map)?;

        Ok(rows)
    }

    /// Parse incremental update data optimized for memory usage
    pub fn parse_incremental_update_data(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_id: u64,
        before_image: &[bool],
        after_image: &[bool],
        changed_columns_only: bool,
    ) -> Result<Vec<crate::row::row_data::IncrementalUpdate>, ReError> {
        let update_rows = self.parse_update_row_data_list_enhanced(
            cursor,
            table_id,
            before_image,
            after_image,
            true, // Enable difference detection
            None,
        )?;

        let mut incremental_updates = Vec::with_capacity(update_rows.len());
        for mut update_row in update_rows {
            let incremental = update_row.to_incremental_update();
            
            // Only include if there are actual changes or if we want all updates
            if !changed_columns_only || incremental.changed_count() > 0 {
                incremental_updates.push(incremental);
            }
        }

        Ok(incremental_updates)
    }

    /// Parse a single row
    fn parse_row(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_map: &TableMapEvent,
        columns_present: &[bool],
        cells_included: usize,
    ) -> Result<RowData, ReError> {
        let start_time = std::time::Instant::now();
        let start_pos = cursor.position();
        
        let result = if self.enable_optimizations {
            // Use optimized zero-copy bitmap parsing
            let null_bitmap_data = read_bitmap_zero_copy(cursor, cells_included)?;
            self.optimized_parser.parse_row_optimized(
                cursor,
                table_map,
                columns_present,
                &null_bitmap_data,
                cells_included,
            )
        } else {
            // Use legacy parsing for compatibility
            self.parse_row_legacy(cursor, table_map, columns_present, cells_included)
        };
        
        // Update statistics
        let end_pos = cursor.position();
        let bytes_processed = end_pos - start_pos;
        let parse_time_duration = start_time.elapsed();
        let parse_time_ns = parse_time_duration.as_nanos() as u64;
        
        // Update legacy stats for backward compatibility
        self.stats.add_row(bytes_processed, parse_time_ns);
        
        // Update comprehensive monitoring if row parsing was successful
        if let Ok(ref row_data) = result {
            self.monitor.record_row_parsed(table_map, row_data, parse_time_duration, bytes_processed);
        }
        
        result
    }
    
    /// Legacy row parsing method for compatibility
    fn parse_row_legacy(
        &self,
        cursor: &mut Cursor<&[u8]>,
        table_map: &TableMapEvent,
        columns_present: &[bool],
        cells_included: usize,
    ) -> Result<RowData, ReError> {
        let column_types = table_map.get_column_types();
        let mut row = Vec::with_capacity(column_types.len());
        let null_bitmap = read_bitmap_little_endian(cursor, cells_included)?;

        let mut skipped_columns = 0;
        for i in 0..column_types.len() {
            // Data is missing if binlog_row_image != full
            if !columns_present[i] {
                skipped_columns += 1;
                row.push(None);
            }
            // Column is present and has null value
            else if null_bitmap[i - skipped_columns] {
                row.push(None);
            }
            // Column has data
            else {
                let mut column_type = column_types[i];
                let mut metadata = table_map.column_metadata[i];

                if SrcColumnType::try_from(column_type).unwrap() == SrcColumnType::String {
                    get_actual_string_type(&mut column_type, &mut metadata);
                }

                row.push(Some(parse_cell(cursor, column_type, metadata)?));
            }
        }

        Ok(RowData::new_with_cells(row))
    }

    /// Clear the table cache
    pub fn clear_cache(&self) -> Result<(), ReError> {
        self.table_cache.clear()
    }

    /// Get cache statistics
    pub fn cache_size(&self) -> Result<usize, ReError> {
        self.table_cache.size()
    }
    
    /// Parse row data list for INSERT events with handler notifications
    pub fn parse_insert_rows(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_id: u64,
        columns_present: &[bool],
    ) -> Result<Vec<RowData>, ReError> {
        let table_map = self.get_table_map(table_id)?
            .ok_or_else(|| ReError::String(TABLE_MAP_NOT_FOUND.to_string()))?;

        // Notify handlers that table processing is starting
        if let Err(e) = self.event_handlers.process_table_start(&table_map) {
            self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
            self.monitor.record_error(&e, Some(&table_map), false);
            return Err(e);
        }

        let cells_included = get_bits_number(columns_present);
        let mut rows = Vec::new();

        while cursor.has_remaining() {
            let row_result = self.parse_row(cursor, &table_map, columns_present, cells_included);

            match row_result {
                Ok(row) => {
                    // Notify handlers of the insert event
                    if let Err(e) = self.event_handlers.process_insert(&table_map, &row) {
                        self.event_handlers.on_error(&table_map, &e).ok();
                        self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                        self.monitor.record_error(&e, Some(&table_map), false);
                        return Err(e);
                    }
                    rows.push(row);
                }
                Err(ReError::IoError(io_error)) => {
                    // Handle end of file gracefully
                    if io_error.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        error!("IO error during row parsing: {:?}", io_error);
                        self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                        self.monitor.record_error(&ReError::IoError(io_error.kind().into()), Some(&table_map), false);
                        return Err(ReError::IoError(io_error));
                    }
                }
                Err(error) => {
                    error!("Error parsing row: {:?}", error);
                    // Notify handlers of the error
                    self.event_handlers.on_error(&table_map, &error).ok();
                    self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                    self.monitor.record_error(&error, Some(&table_map), false);
                    return Err(error);
                }
            }
        }

        // Notify handlers that table processing is ending
        self.event_handlers.process_table_end(&table_map)?;

        // Record the insert operation in monitoring
        self.monitor.record_insert_operation(&table_map, &rows);

        Ok(rows)
    }
    
    /// Parse row data list for DELETE events with handler notifications
    pub fn parse_delete_rows(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_id: u64,
        columns_present: &[bool],
    ) -> Result<Vec<RowData>, ReError> {
        let table_map = self.get_table_map(table_id)?
            .ok_or_else(|| ReError::String(TABLE_MAP_NOT_FOUND.to_string()))?;

        // Notify handlers that table processing is starting
        if let Err(e) = self.event_handlers.process_table_start(&table_map) {
            self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
            self.monitor.record_error(&e, Some(&table_map), false);
            return Err(e);
        }

        let cells_included = get_bits_number(columns_present);
        let mut rows = Vec::new();

        while cursor.has_remaining() {
            let row_result = self.parse_row(cursor, &table_map, columns_present, cells_included);

            match row_result {
                Ok(row) => {
                    // Notify handlers of the delete event
                    if let Err(e) = self.event_handlers.process_delete(&table_map, &row) {
                        self.event_handlers.on_error(&table_map, &e).ok();
                        self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                        self.monitor.record_error(&e, Some(&table_map), false);
                        return Err(e);
                    }
                    rows.push(row);
                }
                Err(ReError::IoError(io_error)) => {
                    // Handle end of file gracefully
                    if io_error.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        error!("IO error during row parsing: {:?}", io_error);
                        self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                        self.monitor.record_error(&ReError::IoError(io_error.kind().into()), Some(&table_map), false);
                        return Err(ReError::IoError(io_error));
                    }
                }
                Err(error) => {
                    error!("Error parsing row: {:?}", error);
                    // Notify handlers of the error
                    self.event_handlers.on_error(&table_map, &error).ok();
                    self.event_handlers.process_table_end(&table_map).ok(); // Try to cleanup
                    self.monitor.record_error(&error, Some(&table_map), false);
                    return Err(error);
                }
            }
        }

        // Notify handlers that table processing is ending
        self.event_handlers.process_table_end(&table_map)?;

        // Record the delete operation in monitoring
        self.monitor.record_delete_operation(&table_map, &rows);

        Ok(rows)
    }
}

/// Parse row event header information
pub fn parse_head(
    cursor: &mut Cursor<&[u8]>,
    post_header_len: u8,
) -> Result<(u64, u16, u16, Vec<ExtraData>, usize, RowEventVersion), ReError> {
    let table_id = match post_header_len as u32 {
        6 => {
            // Master is of an intermediate source tree before 5.1.4. Id is 4 bytes
            cursor.read_u32::<LittleEndian>()? as u64
        }
        _ => {
            // RW_FLAGS_OFFSET
            cursor.read_u48::<LittleEndian>()? as u64
        }
    };

    let flags = cursor.read_u16::<LittleEndian>()?;
    let _f = Flags::from(flags);

    let (extra_data_length, extra_data, version) = if post_header_len == ROWS_HEADER_LEN_V2 {
        let extra_data_length = cursor.read_u16::<LittleEndian>()?;
        assert!(extra_data_length >= 2);

        let header_len: usize = extra_data_length as usize - 2usize;

        let extra_data = match header_len {
            0 => vec![],
            _ => {
                let mut extra_data_vec = vec![0; header_len];
                cursor.read_exact(&mut extra_data_vec)?;
                let mut extra_data_cursor = Cursor::new(extra_data_vec.as_slice());

                let mut v = vec![];
                while extra_data_cursor.position() < extra_data_cursor.get_ref().len() as u64 {
                    let extra = parse_extra_data(&mut extra_data_cursor)?;
                    v.push(extra);
                }
                v
            }
        };

        (extra_data_length, extra_data, RowEventVersion::V2)
    } else {
        (0, vec![], RowEventVersion::V1)
    };

    let (_, columns_number) = read_len_enc_num(cursor)?;

    Ok((
        table_id,
        flags,
        extra_data_length,
        extra_data,
        columns_number as usize,
        version,
    ))
}

/// Gets number of bits set in a bitmap.
fn get_bits_number(bitmap: &[bool]) -> usize {
    bitmap.iter().filter(|&x| *x).count()
}

fn parse_cell(
    cursor: &mut Cursor<&[u8]>,
    column_type: u8,
    metadata: u16,
) -> Result<SrcColumnValue, ReError> {
    let value = match SrcColumnType::try_from(column_type).unwrap() {
        /* Numeric types. The only place where numbers can be negative */
        SrcColumnType::Tiny => SrcColumnValue::TinyInt(cursor.read_u8()?),
        SrcColumnType::Short => SrcColumnValue::SmallInt(cursor.read_u16::<LittleEndian>()?),
        SrcColumnType::Int24 => SrcColumnValue::MediumInt(cursor.read_u24::<LittleEndian>()?),
        SrcColumnType::Long => SrcColumnValue::Int(cursor.read_u32::<LittleEndian>()?),
        SrcColumnType::LongLong => SrcColumnValue::BigInt(cursor.read_u64::<LittleEndian>()?),
        SrcColumnType::Float => SrcColumnValue::Float(cursor.read_f32::<LittleEndian>()?),
        SrcColumnType::Double => SrcColumnValue::Double(cursor.read_f64::<LittleEndian>()?),
        SrcColumnType::Decimal | SrcColumnType::NewDecimal => {
            SrcColumnValue::Decimal(parse_decimal(cursor, metadata)?)
        }
        /* String types, includes varchar, varbinary & fixed char, binary */
        SrcColumnType::VarString | SrcColumnType::VarChar | SrcColumnType::String => {
            SrcColumnValue::String(parse_string(cursor, metadata)?)
        }
        /* BIT, ENUM, SET types */
        SrcColumnType::Bit => SrcColumnValue::Bit(parse_bit(cursor, metadata)?),
        SrcColumnType::Enum => {
            SrcColumnValue::Enum(cursor.read_uint::<LittleEndian>(metadata as usize)? as u32)
        }
        SrcColumnType::Set => {
            SrcColumnValue::Set(cursor.read_uint::<LittleEndian>(metadata as usize)? as u64)
        }
        /* Blob types. MariaDB always creates BLOB for first three */
        SrcColumnType::TinyBlob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
        SrcColumnType::MediumBlob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
        SrcColumnType::LongBlob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
        SrcColumnType::Blob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
        /* Date and time types */
        SrcColumnType::Year => SrcColumnValue::Year(parse_year(cursor, metadata)?),
        SrcColumnType::Date => SrcColumnValue::Date(parse_date(cursor, metadata)?),
        // Older versions of MySQL.
        SrcColumnType::Time => SrcColumnValue::Time(parse_time(cursor, metadata)?),
        SrcColumnType::Timestamp => SrcColumnValue::Timestamp(parse_timestamp(cursor, metadata)?),
        SrcColumnType::DateTime => SrcColumnValue::DateTime(parse_date_time(cursor, metadata)?),
        // MySQL 5.6.4+ types. Supported from MariaDB 10.1.2.
        SrcColumnType::Time2 => SrcColumnValue::Time(parse_time2(cursor, metadata)?),
        SrcColumnType::Timestamp2 => {
            SrcColumnValue::Timestamp(parse_timestamp2(cursor, metadata)?)
        }
        SrcColumnType::DateTime2 => SrcColumnValue::DateTime(parse_date_time2(cursor, metadata)?),
        /* MySQL-specific data types */
        SrcColumnType::Geometry => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
        SrcColumnType::Json => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
        // Null
        // Bool
        _ => {
            return Err(ReError::String(format!(
                "Parsing column type {:?} is not supported",
                SrcColumnType::try_from(column_type).unwrap()
            )))
        }
    };

    Ok(value)
}

fn parse_extra_data(cursor: &mut Cursor<&[u8]>) -> Result<ExtraData, ReError> {
    let dt = cursor.read_u8()?;
    let d_type = match dt {
        0x00 => ExtraDataType::RW_V_EXTRAINFO_TAG,
        _ => {
            error!("unknown extra data type {}", dt);
            return Err(ReError::String(format!("Unknown extra data type: {}", dt)));
        }
    };
    let check_len = cursor.read_u8()?;
    let val = check_len - EXTRA_ROW_INFO_HDR_BYTES;

    let fmt = cursor.read_u8()?;
    if fmt != val {
        return Err(ReError::String(format!(
            "Extra data format mismatch: expected {}, got {}",
            val, fmt
        )));
    }
    
    let extra_data_format = match fmt {
        0x00 => ExtraDataFormat::NDB,
        0x40 => ExtraDataFormat::OPEN1,
        0x41 => ExtraDataFormat::OPEN2,
        0xff => ExtraDataFormat::MULTI,
        _ => {
            error!("unknown extract data format {}", fmt);
            return Err(ReError::String(format!(
                "Unknown extra data format: {}",
                fmt
            )));
        }
    };

    let payload = read_string(cursor, check_len as usize)?;

    Ok(ExtraData {
        d_type,
        data: Payload::ExtraDataInfo {
            length: check_len,
            format: extra_data_format,
            payload,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_table_map_cache() {
        let cache = TableMapCache::new(2);
        
        // Test empty cache
        assert_eq!(cache.size().unwrap(), 0);
        assert!(!cache.contains(1).unwrap());
        
        // Test insertion
        let table_map = TableMapEvent::default();
        cache.insert(1, table_map.clone()).unwrap();
        assert_eq!(cache.size().unwrap(), 1);
        assert!(cache.contains(1).unwrap());
        
        // Test retrieval
        let retrieved = cache.get(1).unwrap();
        assert!(retrieved.is_some());
        
        // Test cache limit (LRU behavior)
        let table_map2 = TableMapEvent::default();
        let table_map3 = TableMapEvent::default();
        cache.insert(2, table_map2).unwrap();
        cache.insert(3, table_map3).unwrap(); // Should evict table_id 1
        
        assert_eq!(cache.size().unwrap(), 2);
        assert!(!cache.contains(1).unwrap()); // Should be evicted
        assert!(cache.contains(2).unwrap());
        assert!(cache.contains(3).unwrap());
        
        // Test clear
        cache.clear().unwrap();
        assert_eq!(cache.size().unwrap(), 0);
    }

    #[test]
    fn test_row_parser_creation() {
        let parser = RowParser::new(100);
        assert_eq!(parser.cache_size().unwrap(), 0);
        
        let default_parser = RowParser::with_default_cache();
        assert_eq!(default_parser.cache_size().unwrap(), 0);
    }

    #[test]
    fn test_row_parser_optimizations() {
        let mut parser = RowParser::new(100);
        assert!(parser.enable_optimizations);
        
        parser.set_optimizations_enabled(false);
        assert!(!parser.enable_optimizations);
        
        parser.set_optimizations_enabled(true);
        assert!(parser.enable_optimizations);
    }

    #[test]
    fn test_row_parser_stats() {
        let mut parser = RowParser::new(100);
        let stats = parser.get_stats();
        assert_eq!(stats.rows_parsed, 0);
        
        parser.reset_stats();
        let stats_after_reset = parser.get_stats();
        assert_eq!(stats_after_reset.rows_parsed, 0);
    }

    #[test]
    fn test_get_bits_number() {
        let bitmap = vec![true, false, true, true, false];
        assert_eq!(get_bits_number(&bitmap), 3);
        
        let empty_bitmap = vec![];
        assert_eq!(get_bits_number(&empty_bitmap), 0);
        
        let all_false = vec![false, false, false];
        assert_eq!(get_bits_number(&all_false), 0);
        
        let all_true = vec![true, true, true];
        assert_eq!(get_bits_number(&all_true), 3);
    }
}
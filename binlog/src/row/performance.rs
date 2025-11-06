use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use common::binlog::column::column_type::SrcColumnType;
use common::binlog::column::column_value::SrcColumnValue;
use common::err::decode_error::ReError;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::row::actual_string_type::get_actual_string_type;
use crate::row::decimal::parse_decimal;
use crate::row::row_data::RowData;
use crate::utils::{parse_bit, parse_blob, parse_date, parse_date_time, parse_date_time2, parse_string, parse_time, parse_time2, parse_timestamp, parse_timestamp2, parse_year};

/// Zero-copy bitmap processing for null value detection
#[derive(Debug, Clone)]
pub struct ZeroCopyBitmap<'a> {
    data: &'a [u8],
    bit_count: usize,
}

impl<'a> ZeroCopyBitmap<'a> {
    /// Create a new zero-copy bitmap from raw bytes
    pub fn new(data: &'a [u8], bit_count: usize) -> Self {
        Self { data, bit_count }
    }

    /// Check if a specific bit is set (true means null)
    pub fn is_set(&self, bit_index: usize) -> bool {
        if bit_index >= self.bit_count {
            return false;
        }
        
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        
        if byte_index >= self.data.len() {
            return false;
        }
        
        (self.data[byte_index] & (1 << bit_offset)) != 0
    }

    /// Get the number of bits in the bitmap
    pub fn bit_count(&self) -> usize {
        self.bit_count
    }

    /// Get the raw data
    pub fn data(&self) -> &[u8] {
        self.data
    }
}

/// Optimized row data parser with memory layout optimization
#[derive(Debug)]
pub struct OptimizedRowParser {
    /// Pre-allocated buffer for row data to reduce allocations
    row_buffer: Vec<Option<SrcColumnValue>>,
}

impl OptimizedRowParser {
    pub fn new() -> Self {
        Self {
            row_buffer: Vec::new(),
        }
    }

    /// Parse a single row with optimized memory access patterns
    pub fn parse_row_optimized(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        table_map: &TableMapEvent,
        columns_present: &[bool],
        null_bitmap_data: &[u8],
        cells_included: usize,
    ) -> Result<RowData, ReError> {
        let column_types = table_map.get_column_types();
        
        // Pre-allocate or reuse buffer to avoid repeated allocations
        self.row_buffer.clear();
        self.row_buffer.reserve(column_types.len());
        
        // Create zero-copy bitmap for null checking
        let null_bitmap = ZeroCopyBitmap::new(null_bitmap_data, cells_included);

        let mut skipped_columns = 0;
        for i in 0..column_types.len() {
            // Data is missing if binlog_row_image != full
            if !columns_present[i] {
                skipped_columns += 1;
                self.row_buffer.push(None);
                continue;
            }
            
            // Column is present and has null value
            if null_bitmap.is_set(i - skipped_columns) {
                self.row_buffer.push(None);
                continue;
            }

            // Column has data - parse it
            let mut column_type = column_types[i];
            let mut metadata = table_map.column_metadata[i];

            if SrcColumnType::try_from(column_type).unwrap() == SrcColumnType::String {
                get_actual_string_type(&mut column_type, &mut metadata);
            }

            let cell_value = self.parse_cell_optimized(cursor, column_type, metadata)?;
            self.row_buffer.push(Some(cell_value));
        }

        // Move the buffer content to avoid cloning
        let cells = std::mem::take(&mut self.row_buffer);
        Ok(RowData::new_with_cells(cells))
    }

    /// Optimized cell parsing with reduced allocations
    fn parse_cell_optimized(
        &self,
        cursor: &mut Cursor<&[u8]>,
        column_type: u8,
        metadata: u16,
    ) -> Result<SrcColumnValue, ReError> {
        let value = match SrcColumnType::try_from(column_type).unwrap() {
            /* Numeric types - direct reading without intermediate allocations */
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
            /* String types - optimized string parsing */
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
            /* Blob types - optimized blob parsing */
            SrcColumnType::TinyBlob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
            SrcColumnType::MediumBlob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
            SrcColumnType::LongBlob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
            SrcColumnType::Blob => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
            /* Date and time types */
            SrcColumnType::Year => SrcColumnValue::Year(parse_year(cursor, metadata)?),
            SrcColumnType::Date => SrcColumnValue::Date(parse_date(cursor, metadata)?),
            SrcColumnType::Time => SrcColumnValue::Time(parse_time(cursor, metadata)?),
            SrcColumnType::Timestamp => SrcColumnValue::Timestamp(parse_timestamp(cursor, metadata)?),
            SrcColumnType::DateTime => SrcColumnValue::DateTime(parse_date_time(cursor, metadata)?),
            SrcColumnType::Time2 => SrcColumnValue::Time(parse_time2(cursor, metadata)?),
            SrcColumnType::Timestamp2 => {
                SrcColumnValue::Timestamp(parse_timestamp2(cursor, metadata)?)
            }
            SrcColumnType::DateTime2 => SrcColumnValue::DateTime(parse_date_time2(cursor, metadata)?),
            /* MySQL-specific data types */
            SrcColumnType::Geometry => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
            SrcColumnType::Json => SrcColumnValue::Blob(parse_blob(cursor, metadata)?),
            _ => {
                return Err(ReError::String(format!(
                    "Parsing column type {:?} is not supported",
                    SrcColumnType::try_from(column_type).unwrap()
                )))
            }
        };

        Ok(value)
    }
}

impl Default for OptimizedRowParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory-efficient bitmap reader that reads directly from cursor
pub fn read_bitmap_zero_copy(cursor: &mut Cursor<&[u8]>, bit_count: usize) -> Result<Vec<u8>, ReError> {
    let byte_count = (bit_count + 7) / 8; // Round up to nearest byte
    let mut bitmap_data = vec![0u8; byte_count];
    cursor.read_exact(&mut bitmap_data)?;
    Ok(bitmap_data)
}

/// Optimized function to count set bits in a bitmap without creating boolean vector
pub fn count_set_bits_optimized(bitmap_data: &[u8], bit_count: usize) -> usize {
    let mut count = 0;
    let full_bytes = bit_count / 8;
    
    // Count bits in full bytes using bit manipulation
    for &byte in &bitmap_data[..full_bytes] {
        count += byte.count_ones() as usize;
    }
    
    // Handle remaining bits in the last partial byte
    if bit_count % 8 != 0 {
        let remaining_bits = bit_count % 8;
        if let Some(&last_byte) = bitmap_data.get(full_bytes) {
            let mask = (1u8 << remaining_bits) - 1;
            count += (last_byte & mask).count_ones() as usize;
        }
    }
    
    count
}

/// Memory pool for reusing row data structures
#[derive(Debug)]
pub struct RowDataPool {
    pool: Vec<Vec<Option<SrcColumnValue>>>,
    max_pool_size: usize,
}

impl RowDataPool {
    pub fn new(max_pool_size: usize) -> Self {
        Self {
            pool: Vec::with_capacity(max_pool_size),
            max_pool_size,
        }
    }

    /// Get a reusable vector from the pool or create a new one
    pub fn get_row_buffer(&mut self, capacity: usize) -> Vec<Option<SrcColumnValue>> {
        if let Some(mut buffer) = self.pool.pop() {
            buffer.clear();
            buffer.reserve(capacity);
            buffer
        } else {
            Vec::with_capacity(capacity)
        }
    }

    /// Return a vector to the pool for reuse
    pub fn return_row_buffer(&mut self, mut buffer: Vec<Option<SrcColumnValue>>) {
        if self.pool.len() < self.max_pool_size {
            buffer.clear();
            self.pool.push(buffer);
        }
        // If pool is full, just drop the buffer
    }

    /// Get current pool size
    pub fn pool_size(&self) -> usize {
        self.pool.len()
    }

    /// Clear the pool
    pub fn clear(&mut self) {
        self.pool.clear();
    }
}

impl Default for RowDataPool {
    fn default() -> Self {
        Self::new(100) // Default pool size
    }
}

/// Performance statistics for row parsing
#[derive(Debug, Default, Clone)]
pub struct RowParsingStats {
    pub rows_parsed: u64,
    pub bytes_processed: u64,
    pub parse_time_ns: u64,
    pub memory_allocations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl RowParsingStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_row(&mut self, bytes: u64, time_ns: u64) {
        self.rows_parsed += 1;
        self.bytes_processed += bytes;
        self.parse_time_ns += time_ns;
    }

    pub fn add_allocation(&mut self) {
        self.memory_allocations += 1;
    }

    pub fn add_cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    pub fn add_cache_miss(&mut self) {
        self.cache_misses += 1;
    }

    pub fn average_parse_time_ns(&self) -> f64 {
        if self.rows_parsed > 0 {
            self.parse_time_ns as f64 / self.rows_parsed as f64
        } else {
            0.0
        }
    }

    pub fn throughput_rows_per_second(&self, total_time_ns: u64) -> f64 {
        if total_time_ns > 0 {
            (self.rows_parsed as f64 * 1_000_000_000.0) / total_time_ns as f64
        } else {
            0.0
        }
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        let total_accesses = self.cache_hits + self.cache_misses;
        if total_accesses > 0 {
            self.cache_hits as f64 / total_accesses as f64
        } else {
            0.0
        }
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_zero_copy_bitmap() {
        let data = vec![0b10101010, 0b11110000];
        let bitmap = ZeroCopyBitmap::new(&data, 16);
        
        // Test bit checking
        assert!(!bitmap.is_set(0)); // bit 0 is 0
        assert!(bitmap.is_set(1));  // bit 1 is 1
        assert!(!bitmap.is_set(2)); // bit 2 is 0
        assert!(bitmap.is_set(3));  // bit 3 is 1
        
        // Test bits in second byte
        assert!(!bitmap.is_set(8));  // bit 8 is 0
        assert!(!bitmap.is_set(9));  // bit 9 is 0
        assert!(!bitmap.is_set(10)); // bit 10 is 0
        assert!(!bitmap.is_set(11)); // bit 11 is 0
        assert!(bitmap.is_set(12));  // bit 12 is 1
        assert!(bitmap.is_set(13));  // bit 13 is 1
        assert!(bitmap.is_set(14));  // bit 14 is 1
        assert!(bitmap.is_set(15));  // bit 15 is 1
        
        // Test out of bounds
        assert!(!bitmap.is_set(16));
        assert!(!bitmap.is_set(100));
    }

    #[test]
    fn test_count_set_bits_optimized() {
        let data = vec![0b11111111, 0b00001111, 0b10101010];
        
        // Test full bytes
        assert_eq!(count_set_bits_optimized(&data[..1], 8), 8);
        assert_eq!(count_set_bits_optimized(&data[..2], 16), 12);
        
        // Test partial last byte
        assert_eq!(count_set_bits_optimized(&data, 20), 16); // 8 + 4 + 4 bits set
        assert_eq!(count_set_bits_optimized(&data, 18), 14); // 8 + 4 + 2 bits set
    }

    #[test]
    fn test_row_data_pool() {
        let mut pool = RowDataPool::new(2);
        
        // Test getting buffer from empty pool
        let buffer1 = pool.get_row_buffer(10);
        assert_eq!(buffer1.capacity(), 10);
        assert_eq!(pool.pool_size(), 0);
        
        // Test returning buffer to pool
        pool.return_row_buffer(buffer1);
        assert_eq!(pool.pool_size(), 1);
        
        // Test getting buffer from pool
        let buffer2 = pool.get_row_buffer(5);
        assert_eq!(pool.pool_size(), 0);
        
        // Test pool size limit
        pool.return_row_buffer(buffer2);
        pool.return_row_buffer(Vec::new());
        pool.return_row_buffer(Vec::new()); // Should be dropped due to size limit
        assert_eq!(pool.pool_size(), 2);
        
        // Test clear
        pool.clear();
        assert_eq!(pool.pool_size(), 0);
    }

    #[test]
    fn test_row_parsing_stats() {
        let mut stats = RowParsingStats::new();
        
        // Test initial state
        assert_eq!(stats.rows_parsed, 0);
        assert_eq!(stats.average_parse_time_ns(), 0.0);
        assert_eq!(stats.cache_hit_ratio(), 0.0);
        
        // Test adding data
        stats.add_row(100, 1000);
        stats.add_row(200, 2000);
        assert_eq!(stats.rows_parsed, 2);
        assert_eq!(stats.bytes_processed, 300);
        assert_eq!(stats.parse_time_ns, 3000);
        assert_eq!(stats.average_parse_time_ns(), 1500.0);
        
        // Test cache statistics
        stats.add_cache_hit();
        stats.add_cache_hit();
        stats.add_cache_miss();
        assert_eq!(stats.cache_hit_ratio(), 2.0 / 3.0);
        
        // Test throughput calculation
        let throughput = stats.throughput_rows_per_second(1_000_000_000); // 1 second
        assert_eq!(throughput, 2.0);
        
        // Test reset
        stats.reset();
        assert_eq!(stats.rows_parsed, 0);
        assert_eq!(stats.bytes_processed, 0);
    }

    #[test]
    fn test_optimized_row_parser() {
        let mut parser = OptimizedRowParser::new();
        
        // Test that parser can be created and reused
        assert_eq!(parser.row_buffer.len(), 0);
        
        // The parser should be ready for use
        // (Actual parsing tests would require more complex setup with real table map data)
    }
}
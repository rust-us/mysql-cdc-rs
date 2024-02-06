use byteorder::{LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use common::err::decode_error::ReError;
use serde::Serialize;
use tracing::error;
use common::binlog::column::column_type::SrcColumnType;
use common::err::CResult;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::metadata::table_metadata::TableMetadata;
use crate::{
    events::event_header::Header,
};
use crate::binlog_server::{TABLE_MAP, TABLE_MAP_META};
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::BuildType;
use crate::events::declare::log_event::LogEvent;
use crate::events::event_raw::HeaderRef;
use crate::row::decimal::get_meta;
use crate::utils::{read_bitmap_little_endian_bits, read_len_enc_num, read_string};

/// The event has table defition for row events.
/// <a href="https://github.com/mysql/mysql-server/blob/mysql-cluster-8.0.22/libbinlogevents/include/rows_event.h#L521">See more</a>
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct TableMapEvent {
    header: Header,

    /////////////////////////////////////////////////////
    //  post-header
    /////////////////////////////////////////////////////
    /// 操作的表的table_id,  table_id take 6 bytes in buffer
    pub table_id: u64,
    /// 目前版本没有用，都是0，保留给之后的版本使用
    pub flags: u16,

    /////////////////////////////////////////////////////
    //  event-body
    /////////////////////////////////////////////////////
    /// Gets database name of the changed table.  the end with [00] term sign in layout
    schema_length: u8,
    /// schema
    database_name: String,

    /// Gets name of the changed table.  the end with [00] term sign in layout
    table_name_length: u8,
    pub table_name: String,

    /// len encoded integer
    columns_number: u64,

    /// Gets column types of the changed table 字段类型的枚举值， 与  column_metadata_type 对应
    column_types: Vec<u8>,
    /// Gets columns metadata meta 值
    pub column_metadata: Vec<u16>,
    /// Gets columns metadata 字段类型， 每个枚举的值与column_types 对应
    pub column_metadata_type: Vec<SrcColumnType>,

    /// 列信息，包含上述 column_types、column_metadata、column_metadata_type、null_bitmap
    column_infos: Vec<ColumnInfo>,

    /// Gets columns nullability， 用于标识某一列是否允许为 null
    pub null_bitmap: Vec<u8>,

    /// Gets table metadata for MySQL 5.6+
    pub table_metadata: Option<TableMetadata>,

    /// 构造来源： BINLOG、DUMP
    pub build_type: BuildType,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct ColumnInfo {

    name: String,

    /// Gets column types of the changed table
    // Gets column types of the changed table： column_types: Vec<u8>
    b_type: Option<u8>,
    // Gets column types of the changed table： column_types: Vec<SrcColumnTypes>
    c_type: Option<SrcColumnType>,

    /// Gets columns metadata: column_metadata: Vec<SrcColumnTypes>
    meta: u16,

    /// Gets columns nullability: null_bitmap: Vec<u8>
    /// 大于0 则为true，否则为 false
    nullable: u8,

    unsigned: bool,
    pk: bool,
    set_enum_values: Vec<String>,
    charset: u8,

    geo_type: u32,
    // geo_type: u8,
    visibility: bool,
    array: bool,
}

impl Default for TableMapEvent {
    fn default() -> Self {
        TableMapEvent {
            header: Header::default(),
            table_id: 0,
            flags: 0,
            schema_length: 0,
            database_name: "".to_string(),
            table_name_length: 0,
            table_name: "".to_string(),
            columns_number: 0,
            column_types: vec![],
            column_metadata: vec![],
            column_metadata_type: vec![],
            column_infos: vec![],
            null_bitmap: vec![],
            table_metadata: Some(TableMetadata::default()),
            build_type: BuildType::BINLOG,
        }
    }
}

impl TableMapEvent {
    pub fn get_table_id(&self) -> u64 {
        self.table_id
    }

    pub fn get_columns_number(&self) -> u64 {
        self.columns_number
    }

    /// Gets column types of the changed table
    pub fn get_column_types(&self) -> Vec<u8> {
        self.column_types.clone()
    }

    /// Gets column types of the changed table
    pub fn get_column_metadata_type(&self) -> Vec<SrcColumnType> {
        self.column_metadata_type.clone()
    }

    /// 返回解析出的列信息
    pub fn get_column_infos(&self) -> &[ColumnInfo] {
        self.column_infos.as_slice()
    }

    pub fn get_database_name(&self) -> String {
        self.database_name.clone()
    }

    pub fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    pub fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    pub fn set_table_name(&mut self, table_name: String) {
        self.table_name = table_name;
    }
}

impl TableMapEvent {
    pub fn new(header: Header, table_id: u64, flags: u16, schema_length: u8, schema: String,
               table_name_length: u8, table_name: String, column_count: u64,
               column_types: Vec<u8>, column_metadata: Vec<u16>, column_metadata_type: Vec<SrcColumnType>,
               column_infos: Vec<ColumnInfo>,
               null_bitmap: Vec<u8>, table_metadata: Option<TableMetadata>) -> TableMapEvent {

        TableMapEvent {
            header,
            table_id,
            flags,
            schema_length,
            database_name: schema,
            table_name_length,
            table_name,
            columns_number: column_count,
            column_types,
            column_metadata,
            column_metadata_type,
            column_infos,
            null_bitmap,
            table_metadata,
            build_type: BuildType::BINLOG,
        }
    }

    pub fn copy(source: &TableMapEvent) -> Self  {
        TableMapEvent::new(
            source.header.clone(),
            source.table_id.clone(),
            source.flags.clone(),
            source.schema_length.clone(),
            source.database_name.clone(),
            source.table_name_length.clone(),
            source.table_name.clone(),
            source.columns_number.clone(),
            source.column_types.clone(),
            source.column_metadata.clone(),
            source.column_metadata_type.clone(),
            source.column_infos.clone(),
            source.null_bitmap.clone(),
            source.table_metadata.clone(),
        )
    }

    fn parse_metadata(
        cursor: &mut Cursor<&[u8]>,
        column_types: &Vec<u8>,
    ) -> CResult<(u32, Vec<u16>, Vec<SrcColumnType>)> {
        let mut metadata = vec![0u16; column_types.len()];
        let mut metadata_type = Vec::<SrcColumnType>::with_capacity(column_types.len());

        let mut _size: u32 = 0u32;

        // See https://mariadb.com/kb/en/library/rows_event_v1/#column-data-formats
        for idx in 0..column_types.len() {
            let column_type = SrcColumnType::try_from(column_types[idx]).unwrap();

            let column_type = if column_type == SrcColumnType::Array {
                let v = cursor.read_u8()?;
                SrcColumnType::try_from(v).unwrap()
            } else {
                column_type
            };

            let (meta, meta_type) = match column_type {
                // 1 byte metadata
                // SrcColumnTypes::TinyBlob |
                // SrcColumnTypes::MediumBlob |
                // SrcColumnTypes::LongBlob |
                SrcColumnType::Blob => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Blob)
                }
                SrcColumnType::Double => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Double)
                }
                SrcColumnType::Float => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Float)
                }
                SrcColumnType::Geometry => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Geometry)
                }
                SrcColumnType::Time2 => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Time2)
                }
                SrcColumnType::DateTime2 => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::DateTime2)
                }
                SrcColumnType::Timestamp2 => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Timestamp2)
                }
                SrcColumnType::Json => {
                    let meta = cursor.read_u8()?;
                    _size += 1;
                    (meta as u16, SrcColumnType::Json)
                }

                // 2 bytes little endian
                SrcColumnType::Bit => {
                    let meta = cursor.read_u16::<LittleEndian>()?;
                    _size += 2;
                    (meta, /*  u16 --> 2 u8 */ SrcColumnType::Bit)
                }
                SrcColumnType::VarChar => {
                    let meta = cursor.read_u16::<LittleEndian>()?;
                    _size += 2;
                    (meta, SrcColumnType::VarChar)
                }
                SrcColumnType::Decimal |
                SrcColumnType::NewDecimal => {
                    // precision
                    let precision = cursor.read_u8()? as u16;
                    let decimals = cursor.read_u8()?;
                    let x = get_meta(precision, decimals);

                    _size += 2;
                    (x, SrcColumnType::NewDecimal)
                }

                // 2 bytes big endian
                /// log_event.h : The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                /// field size, i.e., the number of bytes in the representation of size of the string: 3 or 4.
                SrcColumnType::Enum | SrcColumnType::Set => {
                    /*
                     * log_event.h : The first byte is always
                     * MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                     * field size, i.e., the number of bytes in the
                     * representation of size of the string: 3 or 4.
                     */
                    // real_type, read_u16::<BigEndian>()?
                    let t = cursor.read_u8()? as u16;
                    let mut x = t << 8;
                    // pack or field length
                    let len = cursor.read_u8()?;
                    x += len as u16;

                    _size += 2;
                    (x, column_type.clone())
                }
                SrcColumnType::VarString => {
                    let t = cursor.read_u8()? as u16;
                    let mut x = t << 8;
                    // pack or field length
                    let len = cursor.read_u8()?;
                    x += len as u16;

                    _size += 2;
                    (x, SrcColumnType::VarString)
                }
                SrcColumnType::String => {
                    let t = cursor.read_u8()? as u16;
                    let mut x = t << 8;
                    // pack or field length
                    let len = cursor.read_u8()?;
                    x += len as u16;

                    _size += 2;
                    (x, SrcColumnType::String)
                }
                // 类型的默认 meta 值， 包含 Tiny, Short, Int24, Long, LongLong...
                _ => (0, column_type.clone()),
            };
            metadata[idx] = meta;
            metadata_type.push(meta_type);
        }

        Ok((_size, metadata, metadata_type))
    }

    fn filled_column_info(table_cache_manager: Option<&TableCacheManager>, schema:&str, table_name:&str,
                              column_count:usize,  column_info_maps: &mut Vec<ColumnInfo>) {
        if table_cache_manager.is_some() {
            let tm = table_cache_manager.unwrap();
            if tm.contains(&table_name) {
                let cache_table_info = tm.get(&table_name).expect(&format!("table_cache_manager get {} error", &table_name));

                let _default_columns = vec![];
                let columns = cache_table_info.get_columns().unwrap_or(&_default_columns);
                if columns.len() == column_count {
                    for idx in 0..column_count {
                        match columns.get(idx) {
                            None => {}
                            Some(column) => {
                                match column_info_maps.get_mut(idx) {
                                    None => {}
                                    Some(column_info) => {column_info.set_name(column.get_name());}
                                }
                            }
                        }
                    }
                } else {
                    error!("TABLE_MAP_EVENT 中，库表{}.{}解析列大小 {} 与元数据列大小 {} 不一致！",
                        &schema, &table_name, column_count, columns.len());
                }
            }
        }
    }
}

pub fn get_real_type(type_: u8, meta: u16) -> u8 {
    todo!()
}

impl Default for ColumnInfo {
    fn default() -> Self {
        ColumnInfo {
            b_type: None,
            c_type: None,
            meta: 0,
            name: "".to_string(),
            unsigned: false,
            pk: false,
            set_enum_values: vec![],
            charset: 0,
            geo_type: 0,
            nullable: 0,
            visibility: false,
            array: false,
        }
    }
}

impl ColumnInfo {
    fn new(b_type: u8) -> Self {
        ColumnInfo {
            b_type: Some(b_type),
            c_type: Some(SrcColumnType::try_from(b_type).unwrap()),
            meta: 0,
            nullable: 0,
            name: "".to_string(),
            unsigned: false,
            pk: false,
            set_enum_values: vec![],
            charset: 0,
            geo_type: 0,
            visibility: false,
            array: false,
        }
    }

    pub fn get_c_type(&self) -> Option<SrcColumnType> {
        self.c_type.clone()
    }

    pub fn get_type(&self) -> Option<u8> {
        self.b_type
    }

    pub fn get_name(&mut self) -> String {
        self.name.clone()
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_enum_values(&mut self, set_enum_values: Vec<String>) {
        self.set_enum_values = set_enum_values;
    }

    pub fn set_unsigned(&mut self, unsigned: bool) {
        self.unsigned = unsigned;
    }

    pub fn set_pk(&mut self, pk: bool) {
        self.pk = pk;
    }

    pub fn set_charset(&mut self, charset: u8) {
        self.charset = charset;
    }

    pub fn set_visibility(&mut self, visibility: bool) {
        self.visibility = visibility;
    }

    pub fn get_meta(&self) -> u16 {
        self.meta
    }

    pub fn set_meta(&mut self, meta: u16) {
        self.meta = meta;
    }

    pub fn set_nullable(&mut self, nullable: u8) {
        self.nullable = nullable;
    }

    pub fn set_geo_type(&mut self, geo_type: u32) {
        self.geo_type = geo_type;
    }
}

impl LogEvent for TableMapEvent {
    fn get_type_name(&self) -> String {
        "TableMapEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,) -> Result<Self, ReError> where Self: Sized {

        let common_header_len = context.borrow().get_format_description().common_header_len;
        let query_post_header_len = context.borrow()
            .get_format_description()
            .get_post_header_len(header.borrow_mut().get_event_type() as usize);

        let mut column_info_maps: Vec<ColumnInfo> = Vec::new();

        /* post-header部分 */
        let table_id = cursor.read_u48::<LittleEndian>()?;

        // Reserved bytes, Reserved for future use; currently always 0
        let flags = cursor.read_u16::<LittleEndian>()?;
        // cursor.seek(SeekFrom::Current(2))?;

        /* event-body部分 */
        // Database name is null terminated
        let schema_length = cursor.read_u8()?;
        let schema = read_string(cursor, schema_length as usize)?;
        // term is le_u8, eq 0
        cursor.seek(SeekFrom::Current(1))?;

        // Table name is null terminated
        let table_name_length = cursor.read_u8()?;
        let table_name = read_string(cursor, table_name_length as usize)?;
        // term is le_u8, eq 0
        let term = cursor.read_u8()?;
        assert_eq!(term, 0);
        // cursor.seek(SeekFrom::Current(1))?;

        // Read column information
        let (_, column_count) = read_len_enc_num(cursor)?;
        let mut /* type is Vec<u8>*/ column_types = vec![0u8; column_count as usize];
        cursor.read_exact(&mut column_types)?;
        for t in &column_types {
            column_info_maps.push(ColumnInfo::new(*t));
        }
        // filled column_info_maps#name
        TableMapEvent::filled_column_info(table_cache_manager, &schema, &table_name, column_count as usize, &mut column_info_maps);

        // parse_metadata len
        let (_, _column_metadata_length) = read_len_enc_num(cursor)?;
        // parse_metadata
        let (_m_size, column_metadata_val, column_metadata) =
            TableMapEvent::parse_metadata(cursor, &column_types)?;
        for idx in 0..column_metadata_val.len() {
            let column_info = column_info_maps.get_mut(idx).unwrap();
            column_info.set_meta(column_metadata_val[idx]);
        }

        let null_bitmap = read_bitmap_little_endian_bits(cursor, column_count as usize)?;
        for idx in 0..column_count as usize {
            if null_bitmap[idx] == 0u8 {
                let bit = null_bitmap[idx];
                let column_info = column_info_maps.get_mut(idx).unwrap();
                column_info.set_nullable(bit);
            }
        }

        let mut table_metadata = None;
        let position = cursor.position() as usize;
        let ref_len = cursor.get_ref().len();
        if position + 4 < ref_len {
            // Table metadata is supported in MySQL 5.6+ and MariaDB 10.5+.
            /// After null_bits field, there are some new fields for extra metadata.
            let shard_column_info_maps = Arc::new(Mutex::new(&mut column_info_maps));

            let mut extra_metadata_vec = vec![0u8; (ref_len - position - 4) as usize];
            cursor.read_exact(&mut extra_metadata_vec)?;
            let mut extra_metadata_cursor = Cursor::new(extra_metadata_vec.as_slice());
            let extra_metadata = TableMetadata::read_extra_metadata(
                &mut extra_metadata_cursor,
                &column_types,
                shard_column_info_maps.clone(),
            ).unwrap();

            // Table metadata is supported in MySQL 5.6+ and MariaDB 10.5+.
            table_metadata = Some(extra_metadata);
        }

        // println!("{:?}", &column_info_maps);

        let checksum = cursor.read_u32::<LittleEndian>()?;
        header.borrow_mut().update_checksum(checksum);

        if let Ok(mut mapping) = TABLE_MAP.lock() {
            mapping.insert(table_id, column_metadata.clone());
        }
        if let Ok(mut mapping) = TABLE_MAP_META.lock() {
            mapping.insert(table_id, column_metadata_val.clone());
        }

        Ok(TableMapEvent {
            header: Header::copy(header),
            table_id,
            flags,
            schema_length,
            database_name: schema.clone(),
            table_name_length,
            table_name: table_name.clone(),
            columns_number: column_count,
            column_types,
            column_metadata: column_metadata_val,
            column_metadata_type: column_metadata,
            column_infos: column_info_maps,
            null_bitmap,
            table_metadata,
            build_type: BuildType::BINLOG,
        })
    }
}


#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::io::Cursor;
    use std::rc::Rc;
    use crate::events::declare::log_event::LogEvent;
    use crate::events::event_header::Header;
    use crate::events::event_raw::HeaderRef;
    use crate::events::log_context::LogContext;
    use crate::events::protocol::table_map_event::TableMapEvent;

    #[test]
    fn test_1() {
        assert_eq!(1, 1);
    }

    #[test]
    fn test_parser() {
        let header: Vec<u8> = vec![
            /* header */203, 140, 129, 101, 19, 1, 0, 0, 0, 60, 0, 0, 0, 165, 4, 0, 0, 0, 0,
        ];
        let payload: Vec<u8> = vec![
            /* payload */90, 0, 0, 0, 0, 0, 1, 0, 4, 116, 101, 115, 116, 0, 9, 105, 110, 116, 95, 116, 97, 98, 108, 101, 0, 6, 1, 2, 9, 3, 8, 1, 0, 63, 1, 1, 0, 196, 100, 206, 107
        ];
        let mut cursor = Cursor::new(payload.as_slice());
        let context = Rc::new(RefCell::new(LogContext::default()));

        let h = Header::parse_v4_header(&header, context.clone()).unwrap();
        let header: HeaderRef = Rc::new(RefCell::new(h));

        let e = TableMapEvent::parse(&mut cursor, header.clone(), context.clone(), None, None).unwrap();
        assert_eq!(e.get_table_name(), "int_table");
        assert_eq!(e.get_database_name(), "test");
    }
}
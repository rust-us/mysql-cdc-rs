use byteorder::ReadBytesExt;
use bytes::Buf;
use nom::{
    bytes::complete::take,
    combinator::map,
    number::complete::{le_u16, le_u32, le_u8},
    IResult,
};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};

use common::err::decode_error::ReError;
use serde::Serialize;

use crate::column::column_type::ColumnType;
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::metadata::table_metadata::TableMetadata;
use crate::{
    events::event_header::Header,
    utils::{pu64, read_fixed_len_string, read_len_enc_num},
};
use crate::binlog_server::{TABLE_MAP, TABLE_MAP_META};
use crate::events::BuildType;
use crate::events::event_raw::HeaderRef;

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
    pub column_metadata_type: Vec<ColumnType>,

    /// Gets columns nullability， 用于标识某一列是否允许为 null
    pub null_bitmap: Vec<u8>,

    /// Gets table metadata for MySQL 5.6+
    pub table_metadata: Option<TableMetadata>,

    /// 构造来源： BINLOG、DUMP
    pub build_type: BuildType,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct ColumnInfo {
    /// Gets column types of the changed table
    // Gets column types of the changed table： column_types: Vec<u8>
    b_type: Option<u8>,
    // Gets column types of the changed table： column_types: Vec<ColumnTypes>
    c_type: Option<ColumnType>,

    /// Gets columns metadata: column_metadata: Vec<ColumnTypes>
    meta: u16,

    /// Gets columns nullability: null_bitmap: Vec<u8>
    /// 大于0 则为true，否则为 false
    nullable: u8,

    name: String,

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
    pub fn get_column_metadata_type(&self) -> Vec<ColumnType> {
        self.column_metadata_type.clone()
    }
}

impl TableMapEvent {
    pub fn new(header: Header, table_id: u64, flags: u16, schema_length: u8, schema: String,
               table_name_length: u8, table_name: String, column_count: u64,
               column_types: Vec<u8>, column_metadata: Vec<u16>, column_metadata_type: Vec<ColumnType>,
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
            null_bitmap,
            table_metadata,
            build_type: BuildType::BINLOG,
        }
    }

    pub fn parse<'a>(
        input: &'a [u8],
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
    ) -> IResult<&'a [u8], TableMapEvent> {
        let _context = context.borrow();

        let common_header_len = _context.get_format_description().common_header_len;
        let query_post_header_len = _context
            .get_format_description()
            .get_post_header_len(header.borrow_mut().get_event_type() as usize);

        let mut column_info_maps: Vec<ColumnInfo> = Vec::new();

        /* post-header部分 */
        let (i, table_id): (&'a [u8], u64) = map(take(6usize), |id_raw: &[u8]| {
            let mut filled = id_raw.to_vec();
            filled.extend(vec![0, 0]);
            pu64(&filled).unwrap().1
        })(input)?;

        // Reserved for future use; currently always 0
        let (i, flags) = le_u16(i)?;

        /* event-body部分 */
        let mut current_event_body_pos = 0u32;
        // event-body 部分长度
        let data_len =
            header.borrow_mut().get_event_length() - (common_header_len + query_post_header_len) as u32;

        // Database name is null terminated
        let (i, (schema_length, schema)) = read_fixed_len_string(i)?;
        let (i, term) = le_u8(i)?;
        assert_eq!(term, 0);
        current_event_body_pos += schema_length as u32 + 1 + 1;

        // Table name is null terminated
        let (i, (table_name_length, table_name)) = read_fixed_len_string(i)?;
        let (i, term) = le_u8(i)?; /* termination null */
        assert_eq!(term, 0);
        current_event_body_pos += table_name_length as u32 + 1 + 1;

        // Read column information
        let (i, (_f_size, column_count)) = read_len_enc_num(i)?;
        current_event_body_pos += _f_size as u32;

        // let mut _column_types: Vec<ColumnTypes> = Vec::new();
        let (i, /* type is Vec<u8>*/ column_types): (&'a [u8], Vec<u8>) =
            map(take(column_count), |s: &[u8]| {
                s.iter()
                    .map(|&t| {
                        // _column_types.push(ColumnTypes::from(t));
                        column_info_maps.push(ColumnInfo::new(t));
                        t
                    })
                    .collect()
            })(i)?;
        current_event_body_pos += column_count as u32;

        // parse_metadata len
        let (i, (_ml_size, _column_metadata_length)) = read_len_enc_num(i)?;
        current_event_body_pos += _ml_size as u32;

        // parse_metadata
        let (i, (_m_size, column_metadata_val, column_metadata)) =
            TableMapEvent::parse_metadata(i, &column_types).unwrap();
        for idx in 0..column_metadata_val.len() {
            let column_info = column_info_maps.get_mut(idx).unwrap();
            column_info.set_meta(column_metadata_val[idx]);
        }
        current_event_body_pos += _m_size;

        let mask_len = (column_count + 7) / 8;
        let (i, null_bits) = map(take(mask_len), |s: &[u8]| s)(i)?;
        let null_bitmap =
            TableMapEvent::read_bitmap_little_endian(null_bits, column_count as usize).unwrap();
        current_event_body_pos += mask_len as u32;

        for idx in 0..column_count as usize {
            if null_bitmap[idx] == 0u8 {
                let bit = null_bitmap[idx];
                let column_info = column_info_maps.get_mut(idx).unwrap();
                column_info.set_nullable(bit);
            }
        }
        // let _null_bitmap = null_bitmap.iter().map(|&t| { t > 0 }).collect::<Vec<bool>>();

        let mut table_metadata = None;
        let i = if data_len > current_event_body_pos + 4 {
            /// After null_bits field, there are some new fields for extra metadata.
            let extra_metadata_len = data_len - current_event_body_pos - 4;
            let (ii, _extra_metadata) = map(take(extra_metadata_len), |s: &[u8]| s)(i)?;
            let shard_column_info_maps = Arc::new(Mutex::new(column_info_maps));
            let extra_metadata = TableMetadata::read_extra_metadata(
                _extra_metadata,
                &column_types,
                shard_column_info_maps.clone(),
            )
            .unwrap();

            // Table metadata is supported in MySQL 5.6+ and MariaDB 10.5+.
            table_metadata = Some(extra_metadata);

            ii
        } else {
            i
        };

        let (i, checksum) = le_u32(i)?;

        if let Ok(mut mapping) = TABLE_MAP.lock() {
            mapping.insert(table_id, column_metadata.clone());
        }
        if let Ok(mut mapping) = TABLE_MAP_META.lock() {
            mapping.insert(table_id, column_metadata_val.clone());
        }

        // todo  column_info
        header.borrow_mut().update_checksum(checksum);
        let e = TableMapEvent {
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
            null_bitmap,
            table_metadata,
            build_type: BuildType::BINLOG,
        };

        Ok((i, e))
    }

    pub fn parse_metadata<'a>(
        input: &'a [u8],
        column_types: &Vec<u8>,
    ) -> IResult<&'a [u8], (u32, Vec<u16>, Vec<ColumnType>)> {
        let mut metadata = vec![0u16; column_types.len()];
        let mut metadata_type = Vec::<ColumnType>::with_capacity(column_types.len());

        let mut source = input;
        let mut _size: u32 = 0u32;

        // See https://mariadb.com/kb/en/library/rows_event_v1/#column-data-formats
        for idx in 0..column_types.len() {
            let column_type = ColumnType::try_from(column_types[idx]).unwrap();

            let (s, column_type) = if column_type == ColumnType::Array {
                let (s, v) = le_u8(source)?;
                (s, ColumnType::try_from(v).unwrap())
            } else {
                (source, column_type)
            };
            source = s;

            let (_source, meta, meta_type) = match column_type {
                // 1 byte metadata
                // ColumnTypes::TinyBlob |
                // ColumnTypes::MediumBlob |
                // ColumnTypes::LongBlob |
                ColumnType::Blob => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Blob)
                }
                ColumnType::Double => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Double)
                }
                ColumnType::Float => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Float)
                }
                ColumnType::Geometry => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Geometry)
                }
                ColumnType::Time2 => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Time2)
                }
                ColumnType::DateTime2 => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::DateTime2)
                }
                ColumnType::Timestamp2 => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Timestamp2)
                }
                ColumnType::Json => {
                    let (source, meta) = map(le_u8, |v| v)(source)?;
                    _size += 1;
                    (source, meta as u16, ColumnType::Json)
                }

                // 2 bytes little endian
                ColumnType::Bit => {
                    let (source, meta) = map(le_u16, |v| v)(source)?;
                    _size += 2;
                    (source, meta, /*  u16 --> 2 u8 */ ColumnType::Bit)
                }
                ColumnType::VarChar => {
                    let (source, meta) = map(le_u16, |v| v)(source)?;
                    _size += 2;
                    (source, meta, ColumnType::VarChar)
                }
                ColumnType::NewDecimal => {
                    // precision
                    let (source, precision) = map(le_u8, |v| v as u16)(source)?;
                    let mut x: u16 = precision << 8;
                    // decimals
                    let (source, decimals) = map(le_u8, |v| v)(source)?;
                    x += decimals as u16;

                    _size += 2;
                    (source, x, ColumnType::NewDecimal)
                }

                // 2 bytes big endian
                /// log_event.h : The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                /// field size, i.e., the number of bytes in the representation of size of the string: 3 or 4.
                ColumnType::Enum | ColumnType::Set => {
                    /*
                     * log_event.h : The first byte is always
                     * MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                     * field size, i.e., the number of bytes in the
                     * representation of size of the string: 3 or 4.
                     */
                    // real_type, read_u16::<BigEndian>()?
                    let (source, t) = map(le_u8, |v| v as u16)(source)?;
                    let mut x = t << 8;
                    // pack or field length
                    let (source, len) = map(le_u8, |v| v)(source)?;
                    x += len as u16;

                    _size += 2;
                    (source, x, column_type.clone())
                }
                ColumnType::VarString => {
                    let (source, t) = map(le_u8, |v| v as u16)(source)?;
                    let mut x = t << 8;
                    // pack or field length
                    let (source, len) = map(le_u8, |v| v)(source)?;
                    x += len as u16;

                    _size += 2;
                    (source, x, ColumnType::VarString)
                }
                ColumnType::String => {
                    let (source, t) = map(le_u8, |v| v as u16)(source)?;
                    let mut x = t << 8;
                    // pack or field length
                    let (source, len) = map(le_u8, |v| v)(source)?;
                    x += len as u16;

                    _size += 2;
                    (source, x, ColumnType::String)
                }
                // 类型的默认 meta 值， 包含 Tiny, Short, Int24, Long, LongLong...
                _ => (source, 0, column_type.clone()),
            };
            metadata[idx] = meta;
            metadata_type.push(meta_type);
            source = _source;
        }

        Ok((source, (_size, metadata, metadata_type)))
    }

    /// Reads bitmap in little-endian bytes order
    fn read_bitmap_little_endian<'a>(
        slice: &'a [u8],
        column_count: usize,
    ) -> Result<Vec<u8>, ReError> {
        let mut result = vec![0; column_count];
        // let mut result = vec![false; bits_number];

        let mask_len = (column_count + 7) / 8;

        let mut cursor = Cursor::new(slice);

        for bit in 0..mask_len {
            let flag = &cursor.read_u8()?;
            let _flag = flag & 0xff;

            if _flag == 0 {
                continue;
            }

            for y in 0..8 {
                let index = (bit << 3) + y;
                if index == column_count {
                    break;
                }
                // result[index] = (value & (1 << y)) > 0;
                result[index] = (_flag & (1 << y));
            }
        }

        Ok(result)
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
            source.null_bitmap.clone(),
            source.table_metadata.clone(),
        )
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
            c_type: Some(ColumnType::try_from(b_type).unwrap()),
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

    pub fn get_c_type(&self) -> Option<ColumnType> {
        self.c_type.clone()
    }

    pub fn get_type(&self) -> Option<u8> {
        self.b_type
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

// impl LogEvent for TableMapEvent {
//     fn get_type_name(&self) -> String {
//         "TableMapEvent".to_string()
//     }
// }

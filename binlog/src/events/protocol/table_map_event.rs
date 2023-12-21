use std::io::{self, BufRead, Cursor, Read, Write};
use std::sync::{Arc, RwLock};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use nom::{AsBytes, bytes::complete::take, combinator::map, IResult, number::complete::{le_u16, le_u32, le_u8}};
use nom::number::complete::le_u24;
use serde::Serialize;
use common::err::DecodeError::ReError;

use crate::{
    events::event::Event,
    events::event_header::Header,
    utils::{read_len_enc_num_with_full_bytes, pu64, string_by_fixed_len},
};
use crate::decoder::event_decoder_impl::TABLE_MAP;
use crate::events::column::column_type::ColumnTypes;
use crate::events::log_context::LogContext;
use crate::utils::read_len_enc_num_with_cursor;

/// The event has table defition for row events.
/// <a href="https://mariadb.com/kb/en/library/table_map_event/">See more</a>
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

    /// Gets column types of the changed table
    column_types: Vec<u8>,

    /// Gets columns metadata
    // pub column_metadata: Vec<u16>,
    pub column_metadata: Vec<ColumnTypes>,

    /// Gets columns nullability
    pub null_bitmap: Vec<u8>,

    // /// Gets table metadata for MySQL 5.6+
    // pub table_metadata: Option<TableMetadata>,

    checksum: u32,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct ColumnInfo {
    /// Gets column types of the changed table
    // Gets column types of the changed table： column_types: Vec<u8>
    b_type: Option<u8>,
    // Gets column types of the changed table： column_types: Vec<ColumnTypes>
    c_type: Option<ColumnTypes>,

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
    geoType: u8,
    visibility: bool,
    array: bool,
}


impl TableMapEvent {
    pub fn get_table_id(&self) -> u64 {
        self.table_id
    }
}

impl TableMapEvent {
    pub fn parse<'a>(input: &'a [u8], header: &Header, context: Arc<RwLock<LogContext>>) -> IResult<&'a [u8], TableMapEvent> {

        let _context = context.read().unwrap();

        let common_header_len = _context.get_format_description().common_header_len;
        let query_post_header_len = _context.get_format_description().get_post_header_len(header.get_event_type() as usize);

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
        let data_len = header.get_event_length()
            - (common_header_len + query_post_header_len) as u32;

        // Database name is null terminated
        let (i, (schema_length, schema)) = string_by_fixed_len(i)?;
        let (i, term) = le_u8(i)?;
        assert_eq!(term, 0);
        current_event_body_pos += schema_length as u32 + 1 + 1;

        // Table name is null terminated
        let (i, (table_name_length, table_name)) = string_by_fixed_len(i)?;
        let (i, term) = le_u8(i)?; /* termination null */
        assert_eq!(term, 0);
        current_event_body_pos += table_name_length as u32 + 1 + 1;

        // Read column information
        let (i, (_f_size, column_count)) = read_len_enc_num_with_full_bytes(i)?;
        current_event_body_pos += _f_size as u32;

        let mut column_info_map: Vec<ColumnInfo> = Vec::new();
        let mut _column_types: Vec<ColumnTypes> = Vec::new();
        let (i, /* type is Vec<u8>*/ column_types): (&'a [u8], Vec<u8>) =
            map(take(column_count), |s: &[u8]| {
                s.iter().map(|&t| {
                    _column_types.push(ColumnTypes::from(t));
                    column_info_map.push(ColumnInfo::new(t));
                    t
                }).collect()
            })(i)?;
        current_event_body_pos += column_count as u32;

        // packedLong
        let (i, (_ml_size, _column_metadata_count)) = read_len_enc_num_with_full_bytes(i)?;
        current_event_body_pos += _ml_size as u32;

        // decode_fields
        let (i, column_metadata) = map(take(_column_metadata_count), |s: &[u8]| {
            let mut used = 0;
            let mut ret = vec![];
            for col in _column_types.iter() {
                let (_, (u, val)) = col.decode_fields_def(&s[used..]).unwrap();
                used = used + u;
                ret.push(val);
            }
            ret
        })(i)?;
        current_event_body_pos += _column_metadata_count as u32;
        // // or
        // let (i, (_m_size, column_metadata)) = TableMapEvent::parse_metadata(i, &column_types).unwrap();
        // for idx in 0..column_metadata.len() {
        //     let column_info = column_info_map.get_mut(idx).unwrap();
        //     column_info.set_meta(column_metadata[idx].meta());
        // }
        // current_event_body_pos += _m_size as u32;

        let mask_len = (column_count + 7) / 8;
        let (i, null_bitmap) = map(take(mask_len), |s: &[u8]| s.to_vec())(i)?;
        // let (i, null_bits) = map(take(mask_len), |s: &[u8]| s)(i)?;
        // let null_bitmap = TableMapEvent::read_bitmap_little_endian(
        //     null_bits, column_count as usize).unwrap();
        current_event_body_pos += mask_len as u32;

        // for idx in 0..column_count as usize {
        //     if null_bitmap[idx] == 0u8 {
        //         let bit = null_bitmap[idx];
        //         let column_info = column_info_map.get_mut(idx).unwrap();
        //         column_info.set_nullable(bit);
        //     }
        // }
        // let _null_bitmap = null_bitmap.iter().map(|&t| { t > 0 }).collect::<Vec<bool>>();

        let i = if data_len > current_event_body_pos + 4 {
            /// After null_bits field, there are some new fields for extra metadata.
            let extra_metadata_len = data_len - current_event_body_pos;
            let (ii, extra_metadata) = map(take(extra_metadata_len), |s: &[u8]| s)(input)?;
            let em = TableMapEvent::read_extra_metadata(extra_metadata).unwrap();

            ii
        } else {
            i
        };

        let (i, checksum) = le_u32(i)?;

        // let column_metadata_to_type = column_metadata.iter().map(|&t| {
        //             ColumnTypes::from(t as u8)
        //         }).collect::<Vec<ColumnTypes>>();
        if let Ok(mut mapping) = TABLE_MAP.lock() {
            mapping.insert(table_id, column_metadata.clone());
        }

        let e = TableMapEvent {
            header: Header::copy_and_get(&header, 1, checksum, Vec::new()),
            table_id,
            flags,
            schema_length,
            database_name: schema.clone(),
            table_name_length,
            table_name: table_name.clone(),
            columns_number: column_count,
            column_types,
            column_metadata,
            null_bitmap,
            checksum,
        };

        Ok((i, e))
    }

    pub fn parse_metadata<'a>(input: &'a [u8], column_types: &Vec<u8>)
                                 -> IResult<&'a [u8], (u32, Vec<u16>)> {
        let mut metadata = vec![0u16; column_types.len()];

        let mut source = input;
        let mut _size: u32 = 0u32;

        // See https://mariadb.com/kb/en/library/rows_event_v1/#column-data-formats
        for idx in 0..column_types.len() {
            let column_type = ColumnTypes::from(column_types[idx]);

            let (_source, meta) = match column_type {
                // 1 byte metadata
                ColumnTypes::TinyBlob |
                ColumnTypes::MediumBlob |
                ColumnTypes::LongBlob |
                ColumnTypes::Blob(_) |
                ColumnTypes::Double(_) |
                ColumnTypes::Float(_) |
                ColumnTypes::Geometry(_) |
                ColumnTypes::Time2(_) |
                ColumnTypes::DateTime2(_) |
                ColumnTypes::Timestamp2(_) |
                ColumnTypes::Json(_) => {
                    let (source, meta) = map(le_u8, |v| v as u16)(source)?;

                    _size += 1;
                    (source, meta)
                },

                // 2 bytes little endian
                ColumnTypes::Bit(_, _) |
                ColumnTypes::VarChar(_) => {
                    let (source, meta) = map(le_u16, |v| v as u16)(source)?;
                    // u16 --> 2 u8
                    // metadata_types.push(ColumnTypes::Bit((meta >> 8) as u8, meta as u8));

                    _size += 2;
                    (source, meta)
                },
                ColumnTypes::NewDecimal(_, _) => {
                    // precision
                    let (source, mut precision) = map(le_u8, |v| v as u16)(source)?;
                    precision = precision << 8;
                    // decimals
                    let (source, decimals) = map(le_u8, |v| v as u16)(source)?;

                    precision += decimals;

                    _size += 2;
                    (source, precision)
                },

                // 2 bytes big endian
                ColumnTypes::Enum |
                ColumnTypes::Set |
                ColumnTypes::String(_, _) => {
                    /*
                     * log_event.h : The first byte is always
                     * MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                     * field size, i.e., the number of bytes in the
                     * representation of size of the string: 3 or 4.
                     */
                    // real_type, read_u16::<BigEndian>()?
                    let (source, mut x) = map(le_u8, |v| v as u16)(source)?;
                    x = x << 8;
                    // pack or field length
                    let (source, len) = map(le_u8, |v| v as u16)(source)?;

                    x += len;

                    _size += 2;
                    (source, x)
                },
                _ => (source, 0),
            };
            metadata[idx] = meta;
            source = _source;

            // let (source, binlog_type) = if column_type == ColumnTypes::Array.into() {
            //     let (source, t) = map(le_u8, |v: u8| v)(source)?;
            //
            //     (source, ColumnTypes::from(t))
            // } else {
            //     (source, column_type)
            // };
            //
            // let (s, meta) = match binlog_type {
            //     /// These types store a single byte.
            //     ColumnTypes::Float(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Float(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::Double(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Double(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::Blob(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Blob(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::Geometry(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Geometry(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::Timestamp2(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Timestamp2(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::DateTime2(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::DateTime2(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::Time2(_) => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Time2(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::Json(_) |
            //     ColumnTypes::TinyBlob |
            //     ColumnTypes::MediumBlob |
            //     ColumnTypes::LongBlob  => {
            //         let (source, meta) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(binlog_type.clone());
            //         (source, meta)
            //     },
            //
            //     /// 2 bytes big endian
            //     /// log_event.h : The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
            //     /// field size, i.e., the number of bytes in the representation of size of the string: 3 or 4.
            //     ColumnTypes::Enum |
            //     ColumnTypes::Set => {
            //         // real_type, read_u16::<BigEndian>()?
            //         let (source, mut x) = map(le_u8, |v| v as u16)(source)?;
            //         x = x << 8;
            //         // pack or field length
            //         let (source, len) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(binlog_type.clone());
            //
            //         x += len;
            //
            //         // let (source, x) = map(tuple((le_u8, le_u8)), |(t, len)| {
            //         //     ColumnTypes::String(t, len)
            //         // })(source)?;
            //
            //         (source, x)
            //     },
            //     ColumnTypes::String(_, _) => {
            //         let (source, mut x) = map(le_u8, |v| v as u16)(source)?;
            //         x = x << 8;
            //         let (source, len) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::String(x as u8, len as u8));
            //
            //         x += len;
            //
            //         (source, x)
            //     },
            //
            //     // 2 bytes little endian
            //     ColumnTypes::Bit(_, _) => {
            //         let (source, meta) = map(le_u16, |v| v as u16)(source)?;
            //         // u16 --> 2 u8
            //         metadata_types.push(ColumnTypes::Bit((meta >> 8) as u8, meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::VarChar(_) => {
            //         let (source, meta) = map(le_u16, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::Double(meta as u8));
            //         (source, meta)
            //     },
            //     ColumnTypes::NewDecimal(_, _) => {
            //         // precision
            //         let (source, mut x) = map(le_u8, |v| v as u16)(source)?;
            //         x = x << 8;
            //         // decimals
            //         let (source, decimals) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::NewDecimal(x as u8, decimals as u8));
            //
            //         x += decimals;
            //         (source, x)
            //     },
            //     ColumnTypes::VarString(_, _) => {
            //         let (source, mut t) = map(le_u8, |v| v as u16)(source)?;
            //         t = t << 8;
            //         // pack or field length
            //         let (source, len) = map(le_u8, |v| v as u16)(source)?;
            //         metadata_types.push(ColumnTypes::VarString(t as u8, len as u8));
            //
            //         // t += len;
            //         (source, 0)
            //     },
            //     _ => {
            //         metadata_types.push(binlog_type.clone());
            //         // meta = 0
            //         (source, 0)
            //     },
            // };
        }

        Ok((source, (_size, metadata)))
    }


    /// Reads bitmap in little-endian bytes order
    fn read_bitmap_little_endian<'a>(slice: &'a [u8], column_count: usize)
                                               -> Result<Vec<u8>, ReError> {
        let mut result = vec![0; column_count];
        // let mut result = vec![false; bits_number];

        let mask_len = (column_count + 7) / 8;

        let mut cursor = Cursor::new(slice);

        for bit in 0..mask_len {
            let flag = &cursor.read_u8()?;
            let _flag = flag & 0xff;
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

    fn read_extra_metadata<'a>(slice: &'a [u8]) -> Result<Vec<u8>, ReError> {
        let mut cursor = Cursor::new(slice);
        // let table_id = cursor.read_u48::<LittleEndian>()?;

        let exist_optional_metaData = false;
        loop {
            if !cursor.has_remaining() {
                break;
            }

            // optional metadata fields
            let _type = cursor.get_u8();
            let len = read_len_enc_num_with_cursor(&mut cursor);
        }

        Ok(Vec::from("".as_bytes()))
    }
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
            geoType: 0,
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
            c_type: Some(ColumnTypes::from(b_type)),
            meta: 0,
            nullable: 0,
            name: "".to_string(),
            unsigned: false,
            pk: false,
            set_enum_values: vec![],
            charset: 0,
            geoType: 0,
            visibility: false,
            array: false,
        }
    }

    pub fn set_meta(&mut self, meta: u16) {
        self.meta = meta;
    }

    pub fn set_nullable(&mut self, nullable: u8) {
        self.nullable = nullable;
    }
}
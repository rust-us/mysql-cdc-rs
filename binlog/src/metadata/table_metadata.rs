use std::cell::RefCell;
use std::io;
use std::io::{Cursor, Read};
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use byteorder::ReadBytesExt;
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::column::column_type::ColumnTypes;
use crate::events::protocol::table_map_event::ColumnInfo;
use crate::metadata::default_charset::DefaultCharset;
use crate::metadata::metadata_type::MetadataType;
use crate::utils::{read_len_enc_num_with_cursor, read_len_enc_str_with_cursor};

/// Contains metadata for table columns.
///
/// <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html">See more</a>
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct TableMetadata {
    /// Gets signedness of numeric colums.
    pub signedness: Option<Vec<bool>>,

    /// Gets charsets of character columns.
    pub default_charset: Option<DefaultCharset>,

    /// Gets charsets of character columns.
    pub column_charsets: Option<Vec<u32>>,

    /// Gets column names.
    pub column_names: Option<Vec<String>>,

    /// Gets string values of SET columns.
    pub set_string_values: Option<Vec<Vec<String>>>,

    /// Gets string values of ENUM columns
    pub enum_string_values: Option<Vec<Vec<String>>>,

    /// Gets real types of geometry columns.
    pub geometry_types: Option<Vec<u32>>,

    /// Gets primary keys without prefixes.
    pub simple_primary_keys: Option<Vec<u32>>,

    /// Gets primary keys with prefixes.
    pub primary_keys_with_prefix: Option<Vec<(u32, u32)>>,

    /// Gets charsets of ENUM and SET columns.
    pub enum_and_set_default_charset: Option<DefaultCharset>,

    /// Gets charsets of ENUM and SET columns.
    pub enum_and_set_column_charsets: Option<Vec<u32>>,

    /// Gets visibility attribute of columns.
    pub column_visibility: Option<Vec<bool>>,
}

impl TableMetadata {
    pub fn read_extra_metadata<'a>(slice: &'a [u8], column_types: &[u8], shard_column_info_maps: Arc<Mutex<Vec<ColumnInfo>>>) -> Result<Self, ReError> {
        let mut signedness = None;
        let mut default_charset = None;
        let mut column_charsets = None;
        let mut column_names = None;
        let mut set_string_values = None;
        let mut enum_string_values = None;
        let mut geometry_types = None;
        let mut simple_primary_keys = None;
        let mut primary_keys_with_prefix = None;
        let mut enum_and_set_default_charset = None;
        let mut enum_and_set_column_charsets = None;
        let mut column_visibility = None;

        let mut cursor = Cursor::new(slice);

        let mut exist_optional_meta_data = false;
        // defaultCharsetPairs is default_charset

        while cursor.position() < cursor.get_ref().len() as u64 {
            let type_ = cursor.read_u8()?;
            let metadata_type = MetadataType::try_from(type_).unwrap();
            let (_use_len, metadata_length) = read_len_enc_num_with_cursor(&mut cursor)?;

            let mut metadata = vec![0u8; metadata_length as usize];
            cursor.read_exact(&mut metadata)?;

            let mut buffer = Cursor::new(metadata.as_slice());
            match metadata_type {
                MetadataType::Signedness => {
                    let numeric_count = get_numeric_column_count(column_types)?;
                    signedness = Some(read_bitmap_reverted(&mut buffer, numeric_count, metadata_type, shard_column_info_maps.clone())?);
                }
                MetadataType::DefaultCharset => {
                    default_charset = Some(parse_default_charser(&mut buffer)?);
                }
                MetadataType::ColumnCharset => {
                    // parse_column_charset
                    column_charsets = Some(parse_int_array(&mut buffer, None)?);
                }
                MetadataType::ColumnName => {
                    // set @@global.binlog_row_metadata='FULL'
                    // 主要是补充列名相关信息
                    exist_optional_meta_data = true;
                    column_names = Some(parse_string_array(&mut buffer, shard_column_info_maps.clone())?);
                }
                MetadataType::SetStrValue => {
                    set_string_values = Some(parse_type_values(&mut buffer, true, shard_column_info_maps.clone())?);
                }
                MetadataType::EnumStrValue => {
                    enum_string_values = Some(parse_type_values(&mut buffer, false, shard_column_info_maps.clone())?);
                }
                MetadataType::GeometryType => {
                    geometry_types = Some(parse_int_array(&mut buffer, Some(shard_column_info_maps.clone()))?);
                }
                MetadataType::SimplePrimaryKey => {
                    // stores primary key's column information extracted from field.
                    // Each column has an index and a prefix which are stored as a unit_pair.
                    // prefix is always 0 for SIMPLE_PRIMARY_KEY field.
                    simple_primary_keys = Some(parse_int_array(&mut buffer, Some(shard_column_info_maps.clone()))?);
                }
                MetadataType::PrimaryKeyWithPrefix => {
                    primary_keys_with_prefix = Some(parse_int_map(&mut buffer, Some(shard_column_info_maps.clone()))?);
                }
                MetadataType::EnumAndSetDefaultCharset => {
                    enum_and_set_default_charset = Some(parse_default_charser(&mut buffer)?);
                }
                MetadataType::EnumAndSetColumnCharset => {
                    enum_and_set_column_charsets = Some(parse_int_array(&mut buffer, None)?);
                }
                MetadataType::ColumnVisibility => {
                    column_visibility =
                        Some(read_bitmap_reverted(&mut buffer, column_types.len(), metadata_type, shard_column_info_maps.clone())?);
                }
                _ => {}
            }
        }

        if exist_optional_meta_data {
            // if (existOptionalMetaData) {
            //     int index = 0;
            //     int char_col_index = 0;
            //     for (int i = 0; i < columnCnt; i++) {
            //         int cs = -1;
            //         int type = getRealType(columnInfo[i].type, columnInfo[i].meta);
            //         if (is_character_type(type)) {
            //         if (defaultCharsetPairs != null && !defaultCharsetPairs.isEmpty()) {
            //         if (index < defaultCharsetPairs.size()
            //         && char_col_index == defaultCharsetPairs.get(index).col_index) {
            //         cs = defaultCharsetPairs.get(index).col_charset;
            //         index++;
            //         } else {
            //         cs = default_charset;
            //         }
            //
            //         char_col_index++;
            //         } else if (columnCharsets != null) {
            //         cs = columnCharsets.get(index);
            //         index++;
            //         }
            //
            //         columnInfo[i].charset = cs;
            //         }
            //     }
            // }
        }

        Ok(Self {
            signedness,
            default_charset,
            column_charsets,
            column_names,
            set_string_values,
            enum_string_values,
            geometry_types,
            simple_primary_keys,
            primary_keys_with_prefix,
            enum_and_set_default_charset,
            enum_and_set_column_charsets,
            column_visibility,
        })
    }
}

/// 是否为数字列
fn is_numeric_type(column_type: u8) -> bool {
    match ColumnTypes::try_from(column_type).unwrap() {
        ColumnTypes::Tiny |
        ColumnTypes::Short |
        ColumnTypes::Int24 |
        ColumnTypes::Long |
        ColumnTypes::LongLong |
        ColumnTypes::Float(_) |
        ColumnTypes::Double(_) |
        ColumnTypes::NewDecimal(_, _) => true,
        _ => false,
    }
}

/// 计算数字列的个数
fn get_numeric_column_count(column_types: &[u8]) -> Result<usize, ReError> {
    let mut count = 0;

    for i in 0..column_types.len() {
        if is_numeric_type(column_types[i]) {
            count += 1
        }
    }

    Ok(count)
}

/// stores the signedness flags extracted from field
fn read_bitmap_reverted(cursor: &mut Cursor<&[u8]>, bits_number /* is numeric_count */: usize,
                        metadata_type: MetadataType, shard_column_info_maps: Arc<Mutex<Vec<ColumnInfo>>>) -> Result<Vec<bool>, io::Error> {

    let mut result: Vec<bool> = vec![false; bits_number];
    let bytes_number = (bits_number + 7) / 8;
    for i in 0..bytes_number {
        let ut = cursor.read_u8()?;
        for y in 0..8 {
            let index = (i << 3) + y;
            if index == bits_number {
                break;
            }

            // The difference from ReadBitmap is that bits are reverted
            result[index] = (ut & (1 << (7 - y))) > 0;
        }
    }

    /* update column_info */
    if metadata_type == MetadataType::Signedness {
        let colunm_count = shard_column_info_maps.lock().unwrap().len();
        let mut column_info_maps = shard_column_info_maps.lock().unwrap();
        //column count zise equals column_info_maps size
        let mut idx = 0usize;
        for i in 0..colunm_count {
            let item = column_info_maps.get_mut(i);
            if item.is_none() || is_numeric_type(item.unwrap().get_type().unwrap()) {
                let unsigned = result[idx];
                item.unwrap().set_unsigned(unsigned);
                idx += 1;
            }
        }
    }

    Ok(result)
}

/// stores collation numbers extracted from field.
fn parse_default_charser(cursor: &mut Cursor<&[u8]>) -> Result<DefaultCharset, ReError> {
    // get default_charset
    let (_, default_charset) = read_len_enc_num_with_cursor(cursor)?;

    let charset_collations = parse_int_map(cursor, None)?;

    Ok(DefaultCharset::new(
        default_charset as u32,
        charset_collations,
    ))
}

/// stores collation numbers extracted from field.
fn parse_int_array(cursor: &mut Cursor<&[u8]>, shard_column_info_maps: Option<Arc<Mutex<Vec<ColumnInfo>>>>) -> Result<Vec<u32>, ReError> {
    let mut result = Vec::new();

    while cursor.position() < cursor.get_ref().len() as u64 {
        let (_, value) = read_len_enc_num_with_cursor(cursor)?;

        result.push(value as u32);
    }

    /// if GEOMETRY_TYPE
    // int index = 0;
    // for (int i = 0; i < columnCnt; i++) {
    //     if (columnInfo[i].type == LogEvent.MYSQL_TYPE_GEOMETRY) {
    //         columnInfo[i].geoType = datas.get(index);
    //         index++;
    //     }
    // }

    /// if SIMPLE_PRIMARY_KEY
    // int limit = buffer.position() + length;
    // while (buffer.hasRemaining() && buffer.position() < limit) {
    //     int col_index = (int) buffer.getPackedLong();
    //     columnInfo[col_index].pk = true;
    // }

    Ok(result)
}

fn parse_int_map(cursor: &mut Cursor<&[u8]>, shard_column_info_maps: Option<Arc<Mutex<Vec<ColumnInfo>>>>) -> Result<Vec<(u32, u32)>, ReError> {
    let mut result = Vec::new();

    while cursor.position() < cursor.get_ref().len() as u64 {
        let (_, col_index) = read_len_enc_num_with_cursor(cursor)?;
        let (_, col_charset) = read_len_enc_num_with_cursor(cursor)?;
        result.push((col_index as u32, col_charset as u32));
    }

    /// if PRIMARY_KEY_WITH_PREFIX
    ///int limit = buffer.position() + length;
    //         while (buffer.hasRemaining() && buffer.position() < limit) {
    //             int col_index = (int) buffer.getPackedLong();
    //             // prefix length, 比如 char(32)
    //             @SuppressWarnings("unused")
    //             int col_prefix = (int) buffer.getPackedLong();
    //             columnInfo[col_index].pk = true;
    //         }

    Ok(result)
}

fn parse_string_array(cursor: &mut Cursor<&[u8]>, shard_column_info_maps: Arc<Mutex<Vec<ColumnInfo>>>) -> Result<Vec<String>, ReError> {
    let mut result = Vec::new();
    while cursor.position() < cursor.get_ref().len() as u64 {
        let value = read_len_enc_str_with_cursor(cursor)?;

        result.push(value);
    }

    // int index = 0;
    // while (buffer.hasRemaining() && buffer.position() < limit) {
    //     int len = (int) buffer.getPackedLong();
    //     columnInfo[index++].name = buffer.getFixString(len);
    // }

    Ok(result)
}

fn parse_type_values(cursor: &mut Cursor<&[u8]>, set:bool, shard_column_info_maps: Arc<Mutex<Vec<ColumnInfo>>>) -> Result<Vec<Vec<String>>, ReError> {
    let mut result = Vec::new();
    while cursor.position() < cursor.get_ref().len() as u64 {
        let (_, length) = read_len_enc_num_with_cursor(cursor)?;

        let mut type_values = Vec::new();
        for _i in 0..length as usize {
            type_values.push(read_len_enc_str_with_cursor(cursor)?);
        }
        result.push(type_values);
    }

    // int index = 0;
    // for (int i = 0; i < columnCnt; i++) {
    //     if (set && getRealType(columnInfo[i].type, columnInfo[i].meta) == LogEvent.MYSQL_TYPE_SET) {
    //         columnInfo[i].set_enum_values = datas.get(index);
    //         index++;
    //     }
    //
    //     if (!set && getRealType(columnInfo[i].type, columnInfo[i].meta) == LogEvent.MYSQL_TYPE_ENUM) {
    //         columnInfo[i].set_enum_values = datas.get(index);
    //         index++;
    //     }
    // }

    Ok(result)
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
use std::io;
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};
use byteorder::ReadBytesExt;
use nom::Parser;
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::column::column_type::ColumnType;
use crate::events::protocol::table_map_event::{ColumnInfo, get_real_type};
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

impl Default for TableMetadata {
    fn default() -> Self {
        TableMetadata::new(
            None, None, None, None, None, None, None, None, None, None, None, None
        )
    }
}

impl TableMetadata {
    pub fn new(signedness: Option<Vec<bool>>,
               default_charset: Option<DefaultCharset>,
               column_charsets: Option<Vec<u32>>,
               column_names: Option<Vec<String>>,
               set_string_values: Option<Vec<Vec<String>>>,
               enum_string_values: Option<Vec<Vec<String>>>,
               geometry_types: Option<Vec<u32>>,
               simple_primary_keys: Option<Vec<u32>>,
               primary_keys_with_prefix: Option<Vec<(u32, u32)>>,
               enum_and_set_default_charset: Option<DefaultCharset>,
               enum_and_set_column_charsets: Option<Vec<u32>>,
               column_visibility: Option<Vec<bool>>) -> Self {

        TableMetadata {
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
        }
    }

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
                    default_charset = Some(parse_default_charser(&mut buffer, metadata_type)?);
                }
                MetadataType::ColumnCharset => {
                    // parse_column_charset
                    column_charsets = Some(parse_int_array(&mut buffer, metadata_type,  None)?);
                }
                MetadataType::ColumnName => {
                    // set @@global.binlog_row_metadata='FULL'
                    // 主要是补充列名相关信息
                    exist_optional_meta_data = true;
                    column_names = Some(parse_string_array(&mut buffer, metadata_type, shard_column_info_maps.clone())?);
                }
                MetadataType::SetStrValue => {
                    set_string_values = Some(parse_type_values(&mut buffer, metadata_type, true, shard_column_info_maps.clone())?);
                }
                MetadataType::EnumStrValue => {
                    enum_string_values = Some(parse_type_values(&mut buffer, metadata_type, false, shard_column_info_maps.clone())?);
                }
                MetadataType::GeometryType => {
                    geometry_types = Some(parse_int_array(&mut buffer, metadata_type, Some(shard_column_info_maps.clone()))?);
                }
                MetadataType::SimplePrimaryKey => {
                    // stores primary key's column information extracted from field.
                    // Each column has an index and a prefix which are stored as a unit_pair.
                    // prefix is always 0 for SIMPLE_PRIMARY_KEY field.
                    simple_primary_keys = Some(parse_int_array(&mut buffer, metadata_type, Some(shard_column_info_maps.clone()))?);
                }
                MetadataType::PrimaryKeyWithPrefix => {
                    primary_keys_with_prefix = Some(parse_int_map(&mut buffer, metadata_type, Some(shard_column_info_maps.clone()))?);
                }
                MetadataType::EnumAndSetDefaultCharset => {
                    enum_and_set_default_charset = Some(parse_default_charser(&mut buffer, metadata_type)?);
                }
                MetadataType::EnumAndSetColumnCharset => {
                    enum_and_set_column_charsets = Some(parse_int_array(&mut buffer, metadata_type, None)?);
                }
                MetadataType::ColumnVisibility => {
                    column_visibility =
                        Some(read_bitmap_reverted(&mut buffer, column_types.len(), metadata_type, shard_column_info_maps.clone())?);
                }
                _ => {}
            }
        }

        if exist_optional_meta_data && default_charset.is_some() {
            let default_charset_ins: DefaultCharset = default_charset.clone().unwrap();
            /* col_index,col_charset */
            let default_charset_collations_pair : Vec<(u32, u32)> = default_charset_ins.charset_collations_pair;

            let mut index = 0usize;
            let mut char_col_index = 0u32;
            let column_count = column_types.len();

            let mut column_info_maps = shard_column_info_maps.lock().unwrap();
            for i in 0..column_count {
                let cloumn_type = column_info_maps.get(i).unwrap().get_type().unwrap();
                let cloumn_meta = column_info_maps.get(i).unwrap().get_meta();

                let mut cs = 0u8;
                let real_type = get_real_type(cloumn_type, cloumn_meta);
                if is_character_type(real_type) {
                    if default_charset_collations_pair.len() > 0 {
                        let (col_index, col_charset) = default_charset_collations_pair.get(index).unwrap();
                        if index < default_charset_collations_pair.len()
                            && &char_col_index == col_index {
                            cs = *col_charset as u8;
                        } else {
                            cs = default_charset_ins.default_charset as u8;
                        }

                        char_col_index += 1;
                    } else if column_charsets.is_some() {
                        cs = column_charsets.clone().unwrap().get(index).cloned().unwrap() as u8;
                        index += 1;
                    }
                    column_info_maps.get_mut(i).unwrap().set_charset(cs);
                }
            }
        }

        Ok(TableMetadata::new(
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
        ))
    }
}

/// 是否为字符串列
fn is_character_type(column_type: u8) -> bool {
    match ColumnType::try_from(column_type).unwrap() {
        ColumnType::VarChar |
        ColumnType::Blob |
        ColumnType::VarString |
        ColumnType::String => true,
        _ => false,
    }
}

/// 是否为数字列
fn is_numeric_type(column_type: u8) -> bool {
    match ColumnType::try_from(column_type).unwrap() {
        ColumnType::Tiny |
        ColumnType::Short |
        ColumnType::Int24 |
        ColumnType::Long |
        ColumnType::LongLong |
        ColumnType::Float |
        ColumnType::Double |
        ColumnType::NewDecimal => true,
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
    //column count zise equals column_info_maps size
    let colunm_count = shard_column_info_maps.lock().unwrap().len();
    let mut column_info_maps = shard_column_info_maps.lock().unwrap();

    if metadata_type == MetadataType::Signedness {
        let mut idx = 0usize;
        for i in 0..colunm_count {
            let item = column_info_maps.get(i);
            if item.is_some() && is_numeric_type(item.unwrap().get_type().unwrap()) {
                let unsigned = result[idx].clone();
                column_info_maps.get_mut(i).unwrap().set_unsigned(unsigned);
                idx += 1;
            }
        }
    } else if metadata_type == MetadataType::ColumnVisibility {
        for i in 0..colunm_count {
            column_info_maps.get_mut(i).unwrap().set_visibility(result[i].clone());
        }
    }

    Ok(result)
}

/// stores collation numbers extracted from field.
fn parse_default_charser(cursor: &mut Cursor<&[u8]>, metadata_type: MetadataType) -> Result<DefaultCharset, ReError> {
    // get default_charset
    let (_, default_charset) = read_len_enc_num_with_cursor(cursor)?;

    let charset_collations_pair = parse_int_map(cursor, metadata_type, None)?;

    Ok(DefaultCharset::new(
        default_charset as u32,
        charset_collations_pair,
    ))
}

/// stores collation numbers extracted from field.
fn parse_int_array(cursor: &mut Cursor<&[u8]>, metadata_type: MetadataType, shard_column_info_maps: Option<Arc<Mutex<Vec<ColumnInfo>>>>) -> Result<Vec<u32>, ReError> {
    let mut result = Vec::new();

    while cursor.position() < cursor.get_ref().len() as u64 {
        let (_, col_index) = read_len_enc_num_with_cursor(cursor)?;

        result.push(col_index as u32);
    }

    if shard_column_info_maps.is_some() {
        let colunm_count = shard_column_info_maps.iter().len();

        let _binding = shard_column_info_maps.unwrap();
        let mut column_info_maps = _binding.lock().unwrap();
        if metadata_type  == MetadataType::GeometryType {
            let mut idx = 0usize;
            for i in 0..colunm_count {
                let column = column_info_maps.get(i).unwrap();
                let geometry_type: u8 = ColumnType::Geometry.into();
                if column.get_type().unwrap() == geometry_type {
                    column_info_maps.get_mut(i).unwrap().set_geo_type(result[idx].clone());
                    idx += 1;
                }
            }
        } else if metadata_type  == MetadataType::SimplePrimaryKey {
            for col_index in 0..result.len() {
                column_info_maps.get_mut(col_index).unwrap().set_pk(true);
            }
        }
    }

    Ok(result)
}

fn parse_int_map(cursor: &mut Cursor<&[u8]>, metadata_type: MetadataType, shard_column_info_maps: Option<Arc<Mutex<Vec<ColumnInfo>>>>) -> Result<Vec<(u32, u32)>, ReError> {
    let mut result = Vec::new();

    // let (has_map, column_info_maps) = if shard_column_info_maps.is_some() {
    //     (true, Some(shard_column_info_maps.unwrap().lock().unwrap()))
    // } else {
    //     (false, None)
    // };

    while cursor.position() < cursor.get_ref().len() as u64 {
        let (_, col_index) = read_len_enc_num_with_cursor(cursor)?;
        let (_, col_charset) = read_len_enc_num_with_cursor(cursor)?;
        result.push((col_index as u32, col_charset as u32));

        // if has_map && metadata_type  == MetadataType::PrimaryKeyWithPrefix {
        //     column_info_maps.unwrap().get_mut(col_index as usize).unwrap().set_pk(true);
        // }
    }

    Ok(result)
}

fn parse_string_array(cursor: &mut Cursor<&[u8]>, metadata_type: MetadataType, shard_column_info_maps: Arc<Mutex<Vec<ColumnInfo>>>) -> Result<Vec<String>, ReError> {
    let mut result = Vec::new();

    let mut column_info_maps = shard_column_info_maps.lock().unwrap();
    let mut i = 0usize;
    while cursor.position() < cursor.get_ref().len() as u64 {
        let name = read_len_enc_str_with_cursor(cursor)?;
        result.push(name.clone());

        if metadata_type == MetadataType::ColumnName {
            /* update column_info */
            column_info_maps.get_mut(i).unwrap().set_name(name.clone());
        }
        i += 1;
    }

    Ok(result)
}

fn parse_type_values(cursor: &mut Cursor<&[u8]>, metadata_type: MetadataType, set:bool, shard_column_info_maps: Arc<Mutex<Vec<ColumnInfo>>>) -> Result<Vec<Vec<String>>, ReError> {
    let mut result = Vec::new();
    while cursor.position() < cursor.get_ref().len() as u64 {
        let (_, length) = read_len_enc_num_with_cursor(cursor)?;

        let mut type_values = Vec::new();
        for _i in 0..length as usize {
            type_values.push(read_len_enc_str_with_cursor(cursor)?);
        }
        result.push(type_values);
    }

    let mut column_info_maps = shard_column_info_maps.lock().unwrap();
    let colunm_count = column_info_maps.len();
    let mut idx = 0usize;
    for i in 0..colunm_count {
        let item = column_info_maps.get(i).unwrap();

        let real_type = get_real_type(item.get_type().unwrap(), item.get_meta());
        if set && ColumnType::try_from(real_type).unwrap() == ColumnType::Set {
            column_info_maps.get_mut(i).unwrap().set_enum_values(result[idx].clone());
            idx += 1;
        } else if !set && ColumnType::try_from(real_type).unwrap() == ColumnType::Enum {
            column_info_maps.get_mut(i).unwrap().set_enum_values(result[idx].clone());
            idx += 1;
        }
    }

    Ok(result)
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
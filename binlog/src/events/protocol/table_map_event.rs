use nom::{
    bytes::complete::{tag, take},
    combinator::map,
    number::complete::{le_i64, le_u16, le_u32, le_u64, le_u8},
    IResult,
};
use std::ops::Deref;
use std::rc::Rc;
use serde::Serialize;

use crate::{
    mysql::{ColTypes},
    utils::{int_by_length_encoded, pu64, string_by_fixed_len},
    events::event_header::{Header},
    events::event::{Event},
};
use crate::decoder::event_decoder_impl::TABLE_MAP;

/// The event has table defition for row events.
/// <a href="https://mariadb.com/kb/en/library/table_map_event/">See more</a>
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct TableMapEvent {
    header: Header,

    /// Gets id of the changed table,  table_id take 6 bytes in buffer
    pub table_id: u64,
    pub flags: u16,

    /// Gets database name of the changed table.  the end with [00] term sign in layout
    pub schema_length: u8,
    pub database_name: String,

    /// Gets name of the changed table.  the end with [00] term sign in layout
    pub table_name_length: u8,
    pub table_name: String,

    /// len encoded integer
    pub columns_number: u64,

    /// Gets column types of the changed table
    pub column_types: Vec<u8>,

    /// Gets columns metadata
    // pub column_metadata_: Vec<u16>,
    pub column_metadata: Vec<ColTypes>,

    /// Gets columns nullability
    pub null_bitmap: Vec<bool>,

    // /// Gets table metadata for MySQL 5.6+
    // pub table_metadata: Option<TableMetadata>,

    pub  checksum: u32,
}

impl TableMapEvent {

    pub fn parse<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], Event> {
        let (i, table_id): (&'a [u8], u64) = map(take(6usize), |id_raw: &[u8]| {
            let mut filled = id_raw.to_vec();
            filled.extend(vec![0, 0]);
            pu64(&filled).unwrap().1
        })(input)?;

        // Reserved for future use; currently always 0
        let (i, flags) = le_u16(i)?;

        // Database name is null terminated
        let (i, (schema_length, schema)) = string_by_fixed_len(i)?;
        let (i, term) = le_u8(i)?;
        assert_eq!(term, 0);

        // Table name is null terminated
        let (i, (table_name_length, table_name)) = string_by_fixed_len(i)?;
        let (i, term) = le_u8(i)?; /* termination null */
        assert_eq!(term, 0);

        let (i, (_, columns_number)) = int_by_length_encoded(i)?;
        let (i, /* type is Vec<ColTypes>*/ column_types): (&'a [u8], Vec<ColTypes>) = map(take(columns_number), |s: &[u8]| {
            s.iter().map(|&t| ColTypes::from_u8(t)).collect()
        })(i)?;

        let (i, (_, _metadata_length)) = int_by_length_encoded(i)?;
        //
        let (i, column_metadata) = map(take(_metadata_length), |s: &[u8]| {
            let mut used = 0;
            let mut ret = vec![];
            for column_types in column_types.iter() {
                let (_, (u, val)) = column_types.parse_def(&s[used..]).unwrap();
                used = used + u;
                ret.push(val);
            }
            ret
        })(i)?;

        let mask_len = (_metadata_length + 7) / 8;
        // null_bitmap
        let (i, null_bits) = map(take(mask_len), |s: &[u8]| s.to_vec())(i)?;

        let (i, checksum) = le_u32(i)?;

        if let Ok(mut mapping) = TABLE_MAP.lock() {
            mapping.insert(table_id, column_metadata.clone());
        }

        // let e = TableMapEvent {
        //     header: Default::default(),
        //     table_id,
        //     flags,
        //     schema_length,
        //     database_name: schema.clone(),
        //     table_name_length,
        //     table_name: table_name.clone(),
        //     columns_number,
        //     column_types: vec![],
        //     column_metadata: column_metadata.clone(),
        //     null_bitmap: vec![],
        //     checksum,
        // };

        Ok((
            i,
            Event::TableMap {
                header: Header::copy_and_get(&header, 1, checksum, Vec::new()),

                table_id,
                flags,
                schema_length,
                schema,
                table_name_length,
                table_name,
                columns_number,
                column_metadata,
                null_bits,
                checksum,
            },
        ))
    }
}
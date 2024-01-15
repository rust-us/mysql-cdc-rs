use nom::{bytes::complete::take, combinator::map, number::complete::le_u8, IResult};
use serde::Serialize;
use tracing::error;
use crate::utils::extract_string;

/// Last event of a statement
pub const STMT_END_F: u8 = 1;

/// Value of the OPTION_NO_FOREIGN_KEY_CHECKS flag in thd->options
pub const NO_FOREIGN_KEY_CHECKS_F: u8 = (1 << 1);

/// Value of the OPTION_RELAXED_UNIQUE_CHECKS flag in thd->options
pub const RELAXED_UNIQUE_CHECKS_F: u8 = (1 << 2);

///
///Indicates that rows in this event are complete, that is contain values
///for all columns of the table.
///
pub const COMPLETE_ROWS_F: u8 = (1 << 3);


///  RW = "RoWs"
pub const RW_MAPID_OFFSET: u8 = 0;
pub const  RW_FLAGS_OFFSET: u8 = 6;
pub const RW_VHLEN_OFFSET: u8 = 8;
pub const  RW_V_TAG_LEN: u8 = 1;
pub const RW_V_EXTRAINFO_TAG: u8 = 0x00;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum RowEventVersion {
    /// These event numbers are used from 5.1.16 and forward The V1 event numbers are used from 5.1.16 until mysql-5.6.
    /// contains WRITE_ROWS_V1, UPDATE_ROWS_V1, DELETE_ROWS_V1,
    V1,

    /// Version 2 of the Row events
    /// contains WriteRows, UpdateRows, DeleteRows
    V2,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Flags {
    /// Last event of a statement
    pub end_of_stmt: bool,
    /// Value of the OPTION_NO_FOREIGN_KEY_CHECKS flag in thd->options
    pub foreign_key_checks: bool,
    /// Value of the OPTION_RELAXED_UNIQUE_CHECKS flag in thd->options
    pub unique_key_checks: bool,
    /// Indicates that rows in this event are complete, that is contain values for all columns of the table.
    pub has_columns: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct ExtraData {
    pub d_type: ExtraDataType,
    pub data: Payload,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum ExtraDataType {
    RW_V_EXTRAINFO_TAG = RW_V_EXTRAINFO_TAG as isize,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum Payload {
    ExtraDataInfo {
        length: u8,
        format: ExtraDataFormat,
        payload: String,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[repr(u8)]
pub enum ExtraDataFormat {
    NDB = 0x00,
    OPEN1 = 0x40,
    OPEN2 = 0x41,
    MULTI = 0xff,
}

// #[derive(Debug, Serialize, PartialEq, Eq, Clone)]
// pub struct Row {
//     pub null_bit_mask: Vec<u8>,
//     pub values: Vec<u8>,
// }
//
// impl TryFrom<u8> for ExtraDataFormat {
//     type Error = (ReError);
//
//     fn try_from(value: u8) -> Result<Self, ReError> {
//         Ok(match value {
//             0x00 => ExtraDataFormat::NDB,
//             0x40 => ExtraDataFormat::OPEN1,
//             0x41 => ExtraDataFormat::OPEN2,
//             0xff => ExtraDataFormat::MULTI,
//             _ => {
//                 log::error!("unknown extract data format {}", value);
//                 ReError::Error(String::from("unknown extract data format:".to_owned() + &*value.to_string()))
//             }
//         })
//     }
// }

impl From<u16> for Flags {
    fn from(flag: u16) -> Self {
        Flags {
            end_of_stmt: (flag >> 0) % 2 == 1,
            foreign_key_checks: (flag >> 1) % 2 == 0,
            unique_key_checks: (flag >> 2) % 2 == 0,
            has_columns: (flag >> 3) % 2 == 0,
        }
    }
}

// del
pub fn parse_extra_data<'a>(input: &'a [u8]) -> IResult<&'a [u8], ExtraData> {
    let (i, d_type) = map(le_u8, |t: u8| match t {
        0x00 => ExtraDataType::RW_V_EXTRAINFO_TAG,
        _ => {
            error!("unknown extra data type {}", t);
            unreachable!()
        }
    })(input)?;
    let (i, length) = le_u8(i)?;
    let (i, extra_data_format) = map(le_u8, |fmt: u8| match fmt {
        0x00 => ExtraDataFormat::NDB,
        0x40 => ExtraDataFormat::OPEN1,
        0x41 => ExtraDataFormat::OPEN2,
        0xff => ExtraDataFormat::MULTI,
        _ => {
            error!("unknown extract data format {}", fmt);
            unreachable!()
        }
    })(i)?;
    let (i, payload) = map(take(length), |s: &[u8]| extract_string(s))(i)?;
    Ok((
        i,
        ExtraData {
            d_type,
            data: Payload::ExtraDataInfo {
                length,
                format: extra_data_format,
                payload,
            },
        },
    ))
}


#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
use sqlparser::ast::{DataType, ExactNumberInfo};
use common::binlog::column::column_type::SrcColumnType;
use crate::row::decimal::get_meta;

pub fn sqlparser_data_type_from(data_type: DataType) -> Option<(SrcColumnType, /* meta */ Option<u16>)> {
    match data_type {
        DataType::Character(_) |
        DataType::Char(_) |
        DataType::CharacterVarying(_) |
        DataType::CharVarying(_) |
        DataType::Varchar(_) |
        DataType::Nvarchar(_) => {
            Some((SrcColumnType::VarChar, None))
        },
        DataType::Uuid => {Some((SrcColumnType::String, None))},
        DataType::CharacterLargeObject(_) |
        DataType::CharLargeObject(_) |
        DataType::Clob(_) => {Some((SrcColumnType::VarString, None))},
        DataType::Binary(_) |
        DataType::Varbinary(_) |
        DataType::Blob(_) |
        DataType::Bytes(_) => {Some((SrcColumnType::Blob, None))},
        DataType::Numeric(exactNumber) |
        DataType::Decimal(exactNumber) |
        DataType::BigNumeric(exactNumber) |
        DataType::BigDecimal(exactNumber) |
        DataType::Dec(exactNumber) => {
            match exactNumber {
                ExactNumberInfo::None => {
                    Some((SrcColumnType::Decimal, None))
                }
                ExactNumberInfo::Precision(p) => {
                    Some((SrcColumnType::Decimal, Some(get_meta(p as u16, 2))))
                }
                ExactNumberInfo::PrecisionAndScale(p, s) => {
                    Some((SrcColumnType::Decimal, Some(get_meta(p as u16, s as u8))))
                }
            }
        },
        DataType::Float(_) => {Some((SrcColumnType::Float, None))},
        DataType::TinyInt(_) |
        DataType::UnsignedTinyInt(_) => {Some((SrcColumnType::Tiny, None))},
        DataType::Int2(_) |
        DataType::UnsignedInt2(_) |
        DataType::SmallInt(_) |
        DataType::UnsignedSmallInt(_) => {Some((SrcColumnType::Short, None))},
        DataType::MediumInt(_) |
        DataType::UnsignedMediumInt(_) => {Some((SrcColumnType::Int24, None))},
        DataType::Int(_) |
        DataType::Int4(_) => {Some((SrcColumnType::Long, None))},
        DataType::Int64 => {Some((SrcColumnType::LongLong, None))},
        DataType::Integer(_) |
        DataType::UnsignedInt(_) |
        DataType::UnsignedInt4(_) |
        DataType::UnsignedInteger(_) => {Some((SrcColumnType::Long, None))},
        DataType::BigInt(_) |
        DataType::UnsignedBigInt(_) |
        DataType::Int8(_) |
        DataType::UnsignedInt8(_) => {Some((SrcColumnType::LongLong, None))},
        DataType::Float4 => {Some((SrcColumnType::Float, None))},
        DataType::Float64 |
        DataType::Real |
        DataType::Float8 |
        DataType::Double |
        DataType::DoublePrecision => {Some((SrcColumnType::Double, None))},
        DataType::Bool |
        DataType::Boolean => {Some((SrcColumnType::Bool, None))},
        DataType::Date => {Some((SrcColumnType::Date, None))},
        DataType::Time(_, _) => {Some((SrcColumnType::Time, None))},
        DataType::Datetime(_) => {Some((SrcColumnType::DateTime, None))},
        DataType::Timestamp(_, _) |
        DataType::Interval => {Some((SrcColumnType::Timestamp, None))},
        DataType::JSON |
        DataType::JSONB => {Some((SrcColumnType::Json, None))},
        DataType::Regclass => {Some((SrcColumnType::String, None))},
        DataType::Text => {Some((SrcColumnType::VarString, None))},
        DataType::String(_) => {Some((SrcColumnType::String, None))},
        DataType::Bytea => {Some((SrcColumnType::Blob, None))},
        DataType::Custom(_, _) => {Some((SrcColumnType::String, None))},
        DataType::Array(_) => {Some((SrcColumnType::Array, None))},
        DataType::Enum(_) => {Some((SrcColumnType::Enum, None))},
        DataType::Set(_) => {Some((SrcColumnType::Set, None))},
        DataType::Struct(_) => {Some((SrcColumnType::Blob, None))},
        DataType::Unspecified => {Some((SrcColumnType::String, None))},
    }
}
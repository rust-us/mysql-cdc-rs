#![allow(non_camel_case_types)]

pub mod b_type;
pub mod utils;
pub mod events;
// pub mod connection;
// pub mod cli;
pub mod decoder;
pub mod metadata;

pub use events::{
    query::{Q_FLAGS2_CODE_VAL, Q_SQL_MODE_CODE_VAL, QueryStatusVar},
    rows::{ExtraData, ExtraDataFormat, Flags, Payload, Row},
};

pub use events::column::column_value::ColumnValues;

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}
#![allow(non_camel_case_types)]

pub mod b_type;
pub mod utils;
pub mod events;
// pub mod connection;
// pub mod cli;
pub mod decoder;
pub mod metadata;
pub mod column;
pub mod row;

pub use events::{
    query::{Q_FLAGS2_CODE_VAL, Q_SQL_MODE_CODE_VAL, QueryStatusVar},
};

pub use row::{
    rows::{ExtraData, ExtraDataFormat, Flags, Payload}
};

pub use column::column_value::ColumnValues;

pub const NULL_TERMINATOR: u8 = 0;

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}

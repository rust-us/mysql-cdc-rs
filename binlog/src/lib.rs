#![allow(non_camel_case_types)]
#![feature(is_terminal)]
#![feature(const_trait_impl)]
#![feature(exact_size_is_empty)]

pub mod b_type;
pub mod utils;
pub mod events;
// pub mod connection;
// pub mod cli;
pub mod decoder;
pub mod metadata;
pub mod column;
pub mod row;
pub mod factory;
pub mod dump;
pub mod relay_log;
pub mod binlog_server;
pub mod binlog_options;
pub mod alias;
pub mod starting_strategy;
pub mod cli;

pub use events::{
    query::{Q_FLAGS2_CODE_VAL, Q_SQL_MODE_CODE_VAL, QueryStatusVar},
};

pub use row::{
    rows::{ExtraData, ExtraDataFormat, Flags, Payload}
};

pub const NULL_TERMINATOR: u8 = 0;

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}


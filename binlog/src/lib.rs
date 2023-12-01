#![allow(non_camel_case_types)]

pub mod b_type;
mod utils;
mod mysql;
mod events;

pub use events::{
    query::{QueryStatusVar, Q_FLAGS2_CODE_VAL, Q_SQL_MODE_CODE_VAL},
    rows::{ExtraData, ExtraDataFormat, Flags, Payload, Row},
};

// #[allow(unused_macros)]
// macro_rules! hex {
//     ($data:literal) => {{
//         let buf = bytes::BytesMut::from_iter(
//             (0..$data.len())
//                 .step_by(2)
//                 .map(|i| u8::from_str_radix(&$data[i..i + 2], 16).unwrap()),
//         );
//         buf
//     }};
// }

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test");
    }
}
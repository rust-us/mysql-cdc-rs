use std::fmt::Debug;
use std::time::Duration;
use byte_unit::{Byte, UnitType};
use mysql_common::serde_json;
use pretty_duration::{pretty_duration, PrettyDurationOptions, PrettyDurationOutputFormat};
use serde::Serialize;
use crate::config::load_style::Format;

/// Duration 的格式化输出
pub fn to_duration_pretty(duration: &Duration) -> String {
    // pretty_duration(
    //     &duration,
    //     Some(PrettyDurationOptions {
    //         output_format: Some(PrettyDurationOutputFormat::Expanded),
    //         singular_labels: None,
    //         plural_labels: None,
    //     })
    // )
    pretty_duration(
        &duration,
        None
    )
}

/// 字节大小 的格式化输出
pub fn to_bytes_len_pretty(len: usize) -> String {
    let byte = Byte::from_u128(len as u128).unwrap();
    let adjusted_byte = byte.get_appropriate_unit(UnitType::Decimal);

    format!("{adjusted_byte:.2}")
}

pub fn to_string_pretty<T: Sized + Serialize + Debug>(f: &Format, val: &T) -> String {
    match f {
        Format::Json => {
            let serde_json = serde_json::to_string_pretty(val);

            match serde_json {
                Ok(v) => {
                    v
                },
                Err(e) => {
                    format!("to_string_pretty Json error:{:?}", val)
                }
            }
        },
        Format::Yaml => {
            let serde_yaml = serde_yaml::to_string(val);

            match serde_yaml {
                Ok(v) => {
                    v
                },
                Err(e) => {
                    format!("to_string_pretty Yaml error:{:?}", val)
                }
            }
        },
        Format::None => {
            format!("{:?}", val)
        }
    }
}
use std::time::Duration;
use byte_unit::{Byte, UnitType};
use pretty_duration::{pretty_duration, PrettyDurationOptions, PrettyDurationOutputFormat};

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
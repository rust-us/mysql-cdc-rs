use std::time::{SystemTime, UNIX_EPOCH};
use chrono::Local;

/// 获取当前时间的秒数
pub fn now() -> u64 {
    // 获取当前时间
    let now = SystemTime::now();

    // 将当前时间与 UNIX 纪元（1970-01-01 00:00:00 UTC）之间的持续时间转换为秒
    let duration_since_epoch = now
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards"); // 如果时间回拨了，这会 panic

    // 获取秒数（如果需要更精确的时间，可以使用 duration_since_epoch.as_millis() 或 .as_nanos()）
    return duration_since_epoch.as_secs();
}

/// 获取当前时间的格式化输出
pub fn now_str() -> String {
    let chrono_time = Local::now();

    return chrono_time.format("%Y-%m-%d %H:%M:%S").to_string();
}
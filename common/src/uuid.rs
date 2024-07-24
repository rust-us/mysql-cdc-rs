use std::time::{SystemTime, UNIX_EPOCH};
use std::hash::{Hash, Hasher};
use fnv::FnvHasher; // 使用FNV哈希算法，因为它通常很快
use rand::Rng;

/// 生成 uuid 字符串，包含当前时间戳
pub fn uuid_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis(); // 使用毫秒级时间戳

    let mut hasher = FnvHasher::default();
    now.hash(&mut hasher);
    let hash_value = hasher.finish();

    // 取哈希值的前几个字节（例如，这里取4个字节，转换为十六进制字符串）
    let hash_str = format!("{:08x}", u32::from_le_bytes(hash_value.to_le_bytes()[..4].try_into().unwrap()));

    // 生成一个随机数，并取其后4位十六进制表示（注意：这里直接格式化随机数）
    let random_num = rand::thread_rng().gen_range(0..0x10000000); // 生成一个32位随机数
    // 取随机数的低16位并格式化为4位十六进制数
    let random_str = format!("{:04x}", random_num & 0xFFFF);

    // 合并时间戳哈希和随机部分（注意：这里我们假设random_str已经是正确的长度）
    hash_str + &random_str
}

#[cfg(test)]
mod tests {
    use crate::uuid::uuid_timestamp;

    #[test]
    fn test_uuid_timestamp() {
        let result = uuid_timestamp();
        assert!(result.len() > 0);

        // 输出可能类似于 "15ddccbeaf8d"
        println!("{}", result);
    }
}

use serde::{Deserialize, Serialize};

/// 源端信息
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SrcMeta {
    // 源端类型
    pub src_type: SrcType,
    // 源端地址
    pub url: String,
    // 源端账号
    pub username: String,
    // 源端密码
    pub password: String,
}

impl SrcMeta {
    pub fn new(src_type: SrcType, url: String, username: String, password: String) -> Self {
        Self {
            src_type,
            url,
            username,
            password
        }
    }
}

/// 源端类型
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SrcType {
    Mysql,
    Postgres,
    Mariadb,
}

impl Default for SrcType {
    fn default() -> Self {
        SrcType::Mysql
    }
}
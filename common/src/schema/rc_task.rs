use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::binlog::src_meta::SrcMeta;

/// 多源 + 多库 + 多表 -> op库#表
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RcTask {
    // 任务id
    pub task_id: String,
    // 任务名
    pub task_name: String,
    // 源端信息
    pub src_info: Vec<SrcInfo>,
    // 目标库名
    pub dst_db_name: String,
    // 目标表名
    pub dst_table_name: String,
    // todo 其它属性....
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SrcInfo {
    // 源端类型、地址、账号、密码
    pub src_meta: SrcMeta,
    // 源端库名
    pub src_db_name: HashSet<String>,
    // 源端表名
    pub src_table_name: HashSet<String>,
}
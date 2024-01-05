#![feature(allocator_api)]

use std::time::Duration;

mod error;
mod cmd;
pub mod session;
mod conn;
mod serde;

pub struct RaftClientConfig {
    pub groups: Vec<RaftServerGroup>,
    // 客户端连接最长存活时间
    pub conn_max_keep_alive: Duration,
    // 客户端连接保持心跳时间
    pub conn_heartbeat_time: Duration,
}

pub struct RaftServerGroup {
    pub nodes: Vec<RaftServerNode>,
}

pub struct RaftServerNode {
    pub addr: String,
    pub leader: bool,
}

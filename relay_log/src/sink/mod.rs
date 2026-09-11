//! 下游输出(Sink)抽象。
//!
//! CDC 闭环链路为: binlog 采集 -> 中继日志落盘 -> Sink 投递下游。
//! 当前提供 stdout / file / null 三种最小实现，
//! 后续可在此基础上扩展 Kafka、Elasticsearch、ClickHouse 等下游。

use common::err::CResult;

use crate::relay_log::RelayLog;

pub mod json_file_sink;
pub mod null_sink;
pub mod stdout_sink;

pub use json_file_sink::JsonFileSink;
pub use null_sink::NullSink;
pub use stdout_sink::StdoutSink;

/// 下游输出抽象。
///
/// 中继日志先本地落盘(保证可恢复)，再通过 Sink 投递到下游。
pub trait RelaySink {
    /// Sink 名称(用于日志与统计)
    fn name(&self) -> &str;

    /// 写入一条中继日志。
    ///
    /// `index` 为中继日志在存储中的自增索引，可作为下游幂等键。
    fn write(&mut self, index: u64, log: &RelayLog) -> CResult<()>;

    /// 刷写缓冲区，默认无操作。
    fn flush(&mut self) -> CResult<()> {
        Ok(())
    }
}

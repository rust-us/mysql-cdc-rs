use common::err::CResult;

use crate::relay_log::RelayLog;
use crate::sink::RelaySink;

/// 丢弃所有数据的 Sink(用于性能压测或临时关闭下游)。
#[derive(Debug, Default)]
pub struct NullSink;

impl NullSink {
    pub fn new() -> Self {
        NullSink
    }
}

impl RelaySink for NullSink {
    fn name(&self) -> &str {
        "null"
    }

    fn write(&mut self, _index: u64, _log: &RelayLog) -> CResult<()> {
        Ok(())
    }
}

use std::io::{self, Write};

use serde::Serialize;

use common::err::CResult;
use common::err::decode_error::ReError;

use crate::relay_log::RelayLog;
use crate::sink::RelaySink;

/// Sink 输出记录(JSON Lines)。
#[derive(Serialize)]
struct SinkRecord<'a> {
    index: u64,
    log: &'a RelayLog,
}

/// 将中继日志以 JSON Lines 输出到标准输出。
#[derive(Debug, Default)]
pub struct StdoutSink;

impl StdoutSink {
    pub fn new() -> Self {
        StdoutSink
    }
}

impl RelaySink for StdoutSink {
    fn name(&self) -> &str {
        "stdout"
    }

    fn write(&mut self, index: u64, log: &RelayLog) -> CResult<()> {
        let line = serde_json::to_string(&SinkRecord { index, log })
            .map_err(|e| ReError::Error(format!("sink serialize err: {}", e)))?;
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        writeln!(lock, "{}", line)?;
        Ok(())
    }

    fn flush(&mut self) -> CResult<()> {
        io::stdout().flush()?;
        Ok(())
    }
}

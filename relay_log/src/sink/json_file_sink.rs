use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

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

/// 将中继日志以 JSON Lines 追加写入文件。
pub struct JsonFileSink {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl JsonFileSink {
    /// 创建文件 Sink，父目录不存在时自动创建。
    pub fn new<P: AsRef<Path>>(path: P) -> CResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl RelaySink for JsonFileSink {
    fn name(&self) -> &str {
        "json_file"
    }

    fn write(&mut self, index: u64, log: &RelayLog) -> CResult<()> {
        let line = serde_json::to_string(&SinkRecord { index, log })
            .map_err(|e| ReError::Error(format!("sink serialize err: {}", e)))?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> CResult<()> {
        self.writer.flush()?;
        Ok(())
    }
}

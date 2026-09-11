//! 位点(checkpoint)持久化与断点续传支持。
//!
//! CDC 服务必须记录"已处理到哪个 binlog 位点"，进程重启后从该位点继续消费，
//! 避免重复消费或数据丢失。
//!
//! 位点文件采用 **临时文件 + rename** 的原子写入方式，保证不会读到半写内容。

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use binlog::events::log_position::LogFilePosition;
use common::err::CResult;
use common::err::decode_error::ReError;

/// 位点文件名。
pub const CHECKPOINT_FILE_NAME: &str = "checkpoint.json";

/// binlog 位点(文件名 + 偏移)。
///
/// `position` 为"下一个待读取事件的起始位置"，即已处理完成的最后一个事件的 `end_log_pos`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogPosition {
    file_name: String,
    position: u64,
}

impl BinlogPosition {
    pub fn new(file_name: impl Into<String>, position: u64) -> Self {
        BinlogPosition {
            file_name: file_name.into(),
            position,
        }
    }

    pub fn file_name(&self) -> &str {
        self.file_name.as_str()
    }

    pub fn position(&self) -> u64 {
        self.position
    }
}

impl From<&LogFilePosition> for BinlogPosition {
    fn from(p: &LogFilePosition) -> Self {
        BinlogPosition::new(p.get_file_name(), p.get_position())
    }
}

/// 已持久化的位点。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    /// binlog 文件名
    file_name: String,

    /// binlog 偏移(下一个待读取事件的起始位置)
    position: u64,

    /// 更新时间(毫秒时间戳)
    updated_at_ms: i64,
}

impl Checkpoint {
    pub fn new(file_name: impl Into<String>, position: u64) -> Self {
        Checkpoint {
            file_name: file_name.into(),
            position,
            updated_at_ms: chrono::Local::now().timestamp_millis(),
        }
    }

    pub fn file_name(&self) -> &str {
        self.file_name.as_str()
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }

    /// 转为位点对象
    pub fn to_binlog_position(&self) -> BinlogPosition {
        BinlogPosition::new(self.file_name.clone(), self.position)
    }
}

/// 位点文件管理器(原子读写)。
#[derive(Debug, Clone)]
pub struct CheckpointManager {
    path: PathBuf,
}

impl CheckpointManager {
    /// 在指定目录下创建(文件路径 = `<dir>/checkpoint.json`)。
    pub fn in_dir<P: AsRef<Path>>(dir: P) -> Self {
        CheckpointManager {
            path: dir.as_ref().join(CHECKPOINT_FILE_NAME),
        }
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        CheckpointManager {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// 读取位点；文件不存在或内容损坏时返回 `None`。
    pub fn load(&self) -> CResult<Option<Checkpoint>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&self.path)?;
        match serde_json::from_str::<Checkpoint>(&content) {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(e) => {
                warn!("checkpoint file {:?} parse err, ignore it: {:?}", self.path, e);
                Ok(None)
            }
        }
    }

    /// 原子写入位点(先写临时文件，再 rename 覆盖)。
    pub fn save(&self, checkpoint: &Checkpoint) -> CResult<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        let content = serde_json::to_string(checkpoint)
            .map_err(|e| ReError::Error(format!("checkpoint serialize err: {}", e)))?;

        let tmp_path = self.path.with_extension("json.tmp");
        fs::write(&tmp_path, content.as_bytes())?;
        fs::rename(&tmp_path, &self.path)?;

        info!(
            "checkpoint saved: {}@{} -> {:?}",
            checkpoint.file_name(),
            checkpoint.position(),
            self.path
        );
        Ok(())
    }

    /// 删除位点文件。
    pub fn clear(&self) -> CResult<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

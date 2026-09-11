use serde::Serialize;

use common::config::load_style::Format;
use common::err::CResult;
use common::err::decode_error::ReError;
use connection::binlog::binlog_subscribe::SubscribeOptions;
use relay_log::pipeline::{DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_PIPELINE_CAPACITY};

/// 下游 Sink 类型
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum SinkKind {
    /// JSON Lines 输出到标准输出
    Stdout,
    /// JSON Lines 追加写入文件
    File,
    /// 丢弃数据
    Null,
}

impl SinkKind {
    pub fn parse(s: &str) -> CResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "stdout" => Ok(SinkKind::Stdout),
            "file" => Ok(SinkKind::File),
            "null" => Ok(SinkKind::Null),
            other => Err(ReError::Error(format!(
                "unknown sink `{}`, expect one of [stdout | file | null]",
                other
            ))),
        }
    }
}

impl Default for SinkKind {
    fn default() -> Self {
        SinkKind::Stdout
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CliOptions {
    /// 是否调试模式
    debug: bool,

    /// 是否输出事件日志
    print_logs: bool,

    /// 事件输出格式
    format: Format,

    /// 下游 Sink 类型
    sink: SinkKind,

    /// file sink 输出路径
    sink_path: Option<String>,

    /// 管线队列容量(背压阈值)
    pipeline_capacity: usize,

    /// 位点落盘间隔(事件数)，0 表示关闭位点持久化
    checkpoint_interval: u64,

    /// 是否读取已有位点进行断点续传
    resume: bool,
}

impl CliOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with(
        debug: bool,
        print_logs: bool,
        format: Format,
        sink: SinkKind,
        sink_path: Option<String>,
        pipeline_capacity: usize,
        checkpoint_interval: u64,
        resume: bool,
    ) -> Self {
        CliOptions {
            debug,
            print_logs,
            format,
            sink,
            sink_path,
            pipeline_capacity: pipeline_capacity.max(1),
            checkpoint_interval,
            resume,
        }
    }

    pub fn is_debug(&self) -> bool {
        self.debug
    }

    pub fn get_sink(&self) -> &SinkKind {
        &self.sink
    }

    pub fn get_sink_path(&self) -> Option<&str> {
        self.sink_path.as_deref()
    }

    pub fn get_pipeline_capacity(&self) -> usize {
        self.pipeline_capacity
    }

    /// 位点落盘间隔；0 表示关闭位点持久化
    pub fn get_checkpoint_interval(&self) -> u64 {
        self.checkpoint_interval
    }

    /// 是否启用位点持久化
    pub fn checkpoint_enabled(&self) -> bool {
        self.checkpoint_interval > 0
    }

    /// 是否读取已有位点断点续传
    pub fn resume_enabled(&self) -> bool {
        self.resume && self.checkpoint_enabled()
    }

    pub fn to_subscribe_options(&self) -> SubscribeOptions {
        SubscribeOptions::new(self.debug, self.print_logs, self.format.clone())
    }
}

impl Default for CliOptions {
    fn default() -> Self {
        CliOptions::new_with(
            false,
            false,
            Format::None,
            SinkKind::Stdout,
            None,
            DEFAULT_PIPELINE_CAPACITY,
            DEFAULT_CHECKPOINT_INTERVAL,
            true,
        )
    }
}

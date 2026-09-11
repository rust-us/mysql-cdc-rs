//! 中继日志处理管线: 有界队列 -> 本地落盘 -> 下游投递 -> 位点持久化。
//!
//! # 关于单线程与背压
//!
//! 中继日志存储(`RelayLogStorage`)内部使用 `Rc<RefCell<..>>`，不是 `Send`，
//! 因此整条管线必须在同一线程内运行。管线通过**有界队列**限制在途事件数量:
//! 队列达到容量上限时先落盘并投递再入队(背压点)，保证内存占用有界；
//! 结合上游"读一批处理一批"的采集方式，最终通过 TCP 接收窗口形成端到端背压。
//!
//! # 位点与断点续传
//!
//! 每条事件处理成功(落盘 + 投递)后记录其 binlog 位点，按 `checkpoint_interval`
//! 周期性地原子写入位点文件，并在 `flush()` 时强制落盘。进程重启后由调用方
//! 读取位点并通过 `BinlogSubscribe::set_start_position` 续传。
//!
//! 注意: 位点是周期性写入的，崩溃时可能落在最后一个位点之后，
//! 因此重启后是 **at-least-once** 语义(可能重复消费少量事件)。

use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};

use tracing::{info, warn};

use binlog::events::binlog_event::BinlogEvent;
use common::err::CResult;
use common::err::decode_error::ReError;

use crate::checkpoint::{BinlogPosition, Checkpoint, CheckpointManager};
use crate::relay_log::{RelayCommand, RelayLog};
use crate::sink::RelaySink;
use crate::storage::relay_log_storage::RelayLogStorage;
use crate::storage::storage_config::StorageConfig;

/// 默认管线队列容量。
pub const DEFAULT_PIPELINE_CAPACITY: usize = 1024;

/// 默认位点落盘间隔(事件数)。
pub const DEFAULT_CHECKPOINT_INTERVAL: u64 = 1000;

/// 管线运行统计。
#[derive(Debug, Default, Clone)]
pub struct PipelineStats {
    /// 收到的事件数
    received: u64,

    /// 已落盘并成功投递下游的事件数
    persisted: u64,

    /// 已跳过的事件数(非行事件，如 FORMAT_DESCRIPTION)
    skipped: u64,

    /// 下游投递失败次数
    sink_errors: u64,

    /// 已写入位点文件的次数
    checkpoints_written: u64,

    /// 最近一条已处理中继日志对应的 binlog position
    last_log_pos: u64,
}

impl PipelineStats {
    /// 收到的事件数
    pub fn get_received(&self) -> u64 {
        self.received
    }

    /// 已落盘并成功投递下游的事件数
    pub fn get_persisted(&self) -> u64 {
        self.persisted
    }

    /// 已跳过的事件数
    pub fn get_skipped(&self) -> u64 {
        self.skipped
    }

    /// 下游投递失败次数
    pub fn get_sink_errors(&self) -> u64 {
        self.sink_errors
    }

    /// 已写入位点文件的次数
    pub fn get_checkpoints_written(&self) -> u64 {
        self.checkpoints_written
    }

    /// 最近一条已处理中继日志对应的 binlog position
    pub fn get_last_log_pos(&self) -> u64 {
        self.last_log_pos
    }
}

/// 中继日志处理管线。
pub struct RelayLogPipeline {
    // 存储配置
    storage_config: StorageConfig,

    // 下游输出
    sink: Box<dyn RelaySink>,

    // 按 "db#table" 维护的目标表存储
    storages: HashMap<String, RelayLogStorage>,

    // 在途事件队列(有界)，事件与其位点成对入队以保持对齐
    queue: VecDeque<(BinlogEvent, BinlogPosition)>,

    // 队列容量(背压阈值)
    capacity: usize,

    // 位点文件管理器(可选)
    checkpoint_manager: Option<CheckpointManager>,

    // 位点落盘间隔(事件数)
    checkpoint_interval: u64,

    // 自上次位点落盘以来处理的事件数
    events_since_checkpoint: u64,

    // 最近一次成功处理的事件位点
    last_position: Option<BinlogPosition>,

    // 最近一次已落盘的位点
    checkpointed_position: Option<BinlogPosition>,

    // 统计
    stats: PipelineStats,
}

impl RelayLogPipeline {
    /// 创建管线，并确保中继日志根目录存在。
    pub fn new(storage_config: StorageConfig, sink: Box<dyn RelaySink>, capacity: usize) -> CResult<Self> {
        let relay_log_dir = storage_config.relay_log_dir().clone();
        if !relay_log_dir.is_empty() {
            std::fs::create_dir_all(&relay_log_dir)?;
        }

        let capacity = capacity.max(1);
        info!(
            "relay log pipeline init, sink: {}, relay_log_dir: {}, capacity: {}",
            sink.name(), relay_log_dir, capacity
        );

        Ok(Self {
            storage_config,
            sink,
            storages: HashMap::new(),
            queue: VecDeque::new(),
            capacity,
            checkpoint_manager: None,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            events_since_checkpoint: 0,
            last_position: None,
            checkpointed_position: None,
            stats: PipelineStats::default(),
        })
    }

    /// 开启位点持久化。
    ///
    /// `interval` 为每处理多少个事件落盘一次位点(最小为 1)。
    pub fn set_checkpoint_manager(&mut self, manager: CheckpointManager, interval: u64) {
        self.checkpoint_interval = interval.max(1);
        self.checkpoint_manager = Some(manager);
    }

    /// 最近一次已落盘的位点。
    pub fn checkpointed_position(&self) -> Option<&BinlogPosition> {
        self.checkpointed_position.as_ref()
    }

    /// 接收一个 binlog 事件及其位点，并**立即**落盘、投递下游并记录位点。
    ///
    /// 中继日志是持久化 WAL，事件必须尽快落盘：若只在队列积满时才处理，
    /// 低流量场景下数据会长期滞留内存，既不落盘也不投递，进程异常退出即丢失。
    /// 真正的背压由上游 `forward_events` 的"读一批处理一批"提供(TCP 窗口)。
    /// `capacity` 仅作为内存占用的安全上界。
    pub fn push(&mut self, event: BinlogEvent, position: BinlogPosition) -> CResult<()> {
        self.stats.received += 1;

        // 内存安全上界：极端情况(如未来提供批量投喂接口)下队列不得无限增长
        if self.queue.len() >= self.capacity {
            self.drain()?;
        }

        self.queue.push_back((event, position));
        // 立即处理，避免数据滞留内存
        self.drain()
    }

    /// 处理队列中的所有事件。
    pub fn drain(&mut self) -> CResult<()> {
        while let Some((event, position)) = self.queue.pop_front() {
            self.handle_event(event, position)?;
        }
        Ok(())
    }

    /// 处理剩余事件、刷写下游并强制落盘位点，用于流结束时收尾。
    pub fn flush(&mut self) -> CResult<()> {
        self.drain()?;
        self.sink.flush()?;
        // 收尾时强制落盘一次位点
        self.persist_checkpoint()?;
        info!("relay log pipeline stats: {:?}", self.stats);
        Ok(())
    }

    /// 当前在途(尚未处理)的事件数。
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// 已打开的目标表存储数量。
    pub fn storage_count(&self) -> usize {
        self.storages.len()
    }

    /// 下游 Sink 名称。
    pub fn sink_name(&self) -> &str {
        self.sink.name()
    }

    /// 运行统计。
    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// 单事件处理: binlog 事件 -> 中继日志 -> 落盘 -> 投递下游 -> 记录位点。
    fn handle_event(&mut self, event: BinlogEvent, position: BinlogPosition) -> CResult<()> {
        let relay_log = RelayLog::from_binlog_event(&event);

        // 非行事件(无实际数据变更)无副作用，可直接推进位点
        if matches!(relay_log.relay_command(), RelayCommand::None) {
            self.last_position = Some(position);
            self.stats.skipped += 1;
            return self.maybe_checkpoint();
        }
        self.stats.last_log_pos = *relay_log.event_log_pos();

        let db_name = relay_log.get_database_name().to_string();
        let table_name = relay_log.get_table_name().to_string();
        let storage_key = format!("{}#{}", db_name, table_name);

        if !self.storages.contains_key(&storage_key) {
            let storage = RelayLogStorage::new(&self.storage_config, db_name, table_name)?;
            self.storages.insert(storage_key.clone(), storage);
        }
        let storage = self
            .storages
            .get_mut(&storage_key)
            .ok_or_else(|| ReError::Error(format!("relay log storage not found: {}", storage_key)))?;

        // 1. 本地落盘(保证进程重启后可恢复)
        let index = storage.append_relay_log(relay_log.clone())?;

        // 2. 投递下游
        let sink_name = self.sink.name().to_string();
        match self.sink.write(index, &relay_log) {
            Ok(()) => {
                self.stats.persisted += 1;
                // 本地已落盘后立即刷下游，保证投递结果对下游可见
                self.sink.flush()?;
                // 仅在本事件成功落盘且投递后才推进位点，
                // 保证失败事件不会被 checkpoint 跳过
                self.last_position = Some(position);
                self.maybe_checkpoint()
            }
            Err(e) => {
                self.stats.sink_errors += 1;
                warn!("sink[{}] write err: {:?}", sink_name, e);
                Err(e)
            }
        }
    }

    /// 按间隔触发位点落盘。
    fn maybe_checkpoint(&mut self) -> CResult<()> {
        if self.checkpoint_manager.is_none() {
            return Ok(());
        }

        self.events_since_checkpoint += 1;
        if self.events_since_checkpoint >= self.checkpoint_interval {
            self.persist_checkpoint()?;
        }
        Ok(())
    }

    /// 将当前位点原子写入位点文件。
    fn persist_checkpoint(&mut self) -> CResult<()> {
        let manager = match self.checkpoint_manager.clone() {
            Some(m) => m,
            None => return Ok(()),
        };
        let position = match self.last_position.clone() {
            Some(p) => p,
            None => return Ok(()),
        };

        manager.save(&Checkpoint::new(position.file_name(), position.position()))?;
        self.checkpointed_position = Some(position);
        self.events_since_checkpoint = 0;
        self.stats.checkpoints_written += 1;
        Ok(())
    }
}

impl Debug for RelayLogPipeline {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayLogPipeline")
            .field("sink", &self.sink.name())
            .field("capacity", &self.capacity)
            .field("pending", &self.queue.len())
            .field("storages", &self.storages.keys().collect::<Vec<_>>())
            .field("checkpointed_position", &self.checkpointed_position)
            .field("stats", &self.stats)
            .finish()
    }
}

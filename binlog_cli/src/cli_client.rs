use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use binlog::binlog_server::BinlogServer;
use common::config::BinlogConfig;
use common::err::CResult;
use common::err::decode_error::ReError;
use common::server::Server;
use connection::binlog::binlog_subscribe::BinlogSubscribe;
use relay_log::checkpoint::{BinlogPosition, CheckpointManager};
use relay_log::pipeline::RelayLogPipeline;
use relay_log::sink::json_file_sink::JsonFileSink;
use relay_log::sink::null_sink::NullSink;
use relay_log::sink::stdout_sink::StdoutSink;
use relay_log::sink::RelaySink;
use relay_log::storage::storage_config::StorageConfig;

use crate::cli_options::{CliOptions, SinkKind};

#[derive(Debug)]
pub struct CliClient {
    cli_options: CliOptions,

    binlog_server: BinlogServer,

    binlog_subscribe: BinlogSubscribe,

    /// 中继日志存储配置
    storage_config: StorageConfig,

    /// 停止标志(优雅停机)
    stop: Arc<AtomicBool>,
}

impl CliClient {
    pub fn new(cli_options: CliOptions, binlog_config: BinlogConfig, storage_config: StorageConfig) -> Self {
        let binlog_server = BinlogServer::new();
        let binlog_subscribe = BinlogSubscribe::new(
            cli_options.is_debug(),
            binlog_config,
            cli_options.to_subscribe_options(),
        );

        CliClient {
            cli_options,
            binlog_server,
            binlog_subscribe,
            storage_config,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 停止标志句柄，供信号处理线程置位以实现优雅停机。
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    /// 构建下游 Sink。
    fn build_sink(&self) -> CResult<Box<dyn RelaySink>> {
        match self.cli_options.get_sink() {
            SinkKind::Stdout => Ok(Box::new(StdoutSink::new())),
            SinkKind::Null => Ok(Box::new(NullSink::new())),
            SinkKind::File => {
                let path = self.cli_options.get_sink_path().ok_or_else(|| {
                    ReError::Error("`--sink-file` is required when `--sink=file`".to_string())
                })?;
                Ok(Box::new(JsonFileSink::new(path)?))
            }
        }
    }

    /// 运行端到端闭环: binlog 采集 -> 中继日志落盘 -> Sink 投递 -> 位点持久化。
    pub fn run(&mut self) -> CResult<()> {
        println!("CliClient start");

        // 位点文件位于中继日志根目录
        let checkpoint_manager = CheckpointManager::in_dir(self.storage_config.relay_log_dir());

        // 断点续传: 存在位点则从其继续(优先于配置中的起始位点)
        if self.cli_options.resume_enabled() {
            match checkpoint_manager.load() {
                Ok(Some(cp)) => {
                    println!("resume from checkpoint: {}@{} (updated_at={})",
                             cp.file_name(), cp.position(), cp.updated_at_ms());
                    self.binlog_subscribe
                        .set_start_position(cp.file_name().to_string(), cp.position());
                }
                Ok(None) => {
                    println!("no checkpoint found, start from configured position");
                }
                Err(e) => {
                    eprintln!("load checkpoint failed: {}, start from configured position", e);
                }
            }
        }

        // 建立 binlog 连接
        self.binlog_subscribe.setup_and_connect()?;

        let log_pos = self.binlog_subscribe.get_log_position();
        println!("load_read_ptr: [{}], pos {} in {}",
                 self.binlog_subscribe.load_read_ptr(), log_pos.get_position(), log_pos.get_file_name());

        // 构建中继日志处理管线
        let mut pipeline = RelayLogPipeline::new(
            self.storage_config.clone(),
            self.build_sink()?,
            self.cli_options.get_pipeline_capacity(),
        )?;

        if self.cli_options.checkpoint_enabled() {
            let interval = self.cli_options.get_checkpoint_interval();
            println!("checkpoint enabled, interval={} events, file: {:?}",
                     interval, checkpoint_manager.path());
            pipeline.set_checkpoint_manager(checkpoint_manager, interval);
        }

        // 采集并逐条送入管线(背压由 forward_events 的批次处理 + 管线有界队列共同保证)
        let stop = Arc::clone(&self.stop);
        self.binlog_subscribe
            .forward_events(stop, |event, position| pipeline.push(event, BinlogPosition::from(&position)))?;

        // 收尾: 处理剩余事件、刷写下游并落盘位点
        pipeline.flush()?;
        println!("relay log pipeline done, stats: {:?}", pipeline.stats());

        Ok(())
    }
}

unsafe impl Send for CliClient {}

#[async_trait::async_trait]
impl Server for CliClient {
    async fn start(&mut self) -> CResult<()> {
        self.binlog_server.start().await?;
        self.run()
    }

    async fn shutdown(&mut self, _graceful: bool) -> CResult<()> {
        println!("CliClient shutdown");
        self.stop.store(true, Ordering::SeqCst);
        Ok(())
    }
}

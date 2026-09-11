use std::path::{Path, PathBuf};

use tracing::info;

use common::err::CResult;
use common::log::tracing_factory::TracingFactory;
use relay_log::checkpoint::{BinlogPosition, CheckpointManager};
use relay_log::pipeline::RelayLogPipeline;
use relay_log::sink::json_file_sink::JsonFileSink;
use relay_log::storage::storage_config::StorageConfig;

use crate::relay_log::test_relay_log;

/// 递归查找中继日志 segment 文件(rlog-*.log)
fn find_segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return files,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(find_segment_files(&path));
        } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("rlog-") && name.ends_with(".log") {
                files.push(path);
            }
        }
    }
    files
}

/// 端到端闭环: binlog 事件 -> 中继日志落盘 -> Sink 投递 -> 位点持久化
#[test]
fn test_pipeline_end_to_end() -> CResult<()> {
    TracingFactory::init_log(true);

    let temp_dir = std::env::temp_dir().join("relay_log_pipeline_e2e");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    // 存储配置: 每条 segment 仅容纳 1 个 entry，覆盖 segment 轮转分支
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir(temp_dir.to_str().unwrap().to_string());
    storage_config.set_max_segment_entries(1);

    // 文件 Sink: 便于校验下游输出
    let sink_path = temp_dir.join("cdc.jsonl");
    let sink = JsonFileSink::new(&sink_path)?;

    // 队列容量 1: 覆盖"队列满先落盘"的背压分支
    let mut pipeline = RelayLogPipeline::new(storage_config, Box::new(sink), 1)?;

    // 位点: 容量 1 -> 每条事件都落盘位点
    let checkpoint_manager = CheckpointManager::in_dir(&temp_dir);
    pipeline.set_checkpoint_manager(checkpoint_manager.clone(), 1);

    let events = test_relay_log::get_table_map_event_write_rows_log_event();
    let row_events: Vec<_> = events.into_iter().filter(|e| e.is_row_event()).collect();
    info!("row events: {}", row_events.len());
    assert!(!row_events.is_empty(), "sample binlog should contain row events");

    let mut last_position = 0u64;
    for (i, event) in row_events.into_iter().enumerate() {
        last_position = 100 * (i as u64 + 1);
        pipeline.push(event, BinlogPosition::new("mysql-bin.000001", last_position))?;
    }
    pipeline.flush()?;

    // 统计校验
    let stats = pipeline.stats();
    info!("pipeline stats: {:?}", stats);
    assert!(stats.get_persisted() > 0, "should persist at least one relay log");
    assert_eq!(0, stats.get_sink_errors(), "sink should not fail");
    assert_eq!(stats.get_received(), stats.get_persisted() + stats.get_skipped());
    assert!(stats.get_checkpoints_written() > 0, "should write checkpoint");

    // 下游有输出且为 JSON Lines
    let content = std::fs::read_to_string(&sink_path)?;
    assert!(!content.is_empty(), "sink file should not be empty");
    assert!(content.contains("\"index\""), "sink record should carry index");
    assert!(content.contains("\"relay_command\""), "sink record should carry relay log body");

    // 本地落盘产生了 segment 文件
    let segments = find_segment_files(&temp_dir);
    info!("segments: {:?}", segments);
    assert!(!segments.is_empty(), "should create relay log segment files");

    // 位点已持久化，且与最后处理的事件位点一致
    let checkpoint = checkpoint_manager.load()?.expect("checkpoint should be saved");
    info!("checkpoint: {:?}", checkpoint);
    assert_eq!("mysql-bin.000001", checkpoint.file_name());
    assert_eq!(last_position, checkpoint.position());
    assert_eq!(last_position, pipeline.checkpointed_position().unwrap().position());

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

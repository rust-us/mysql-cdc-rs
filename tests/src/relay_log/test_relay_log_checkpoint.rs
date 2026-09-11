use tracing::info;

use common::err::CResult;
use common::log::tracing_factory::TracingFactory;
use relay_log::checkpoint::{BinlogPosition, Checkpoint, CheckpointManager};

/// 位点文件的保存 / 覆盖 / 读取 / 清理
#[test]
fn test_checkpoint_save_load_clear() -> CResult<()> {
    TracingFactory::init_log(true);

    let temp_dir = std::env::temp_dir().join("relay_log_checkpoint_test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let manager = CheckpointManager::in_dir(&temp_dir);
    assert_eq!(temp_dir.join("checkpoint.json").as_path(), manager.path());
    assert!(manager.load()?.is_none(), "no checkpoint before save");

    // 保存
    manager.save(&Checkpoint::new("mysql-bin.000007", 12345))?;
    let checkpoint = manager.load()?.expect("checkpoint should exist");
    info!("checkpoint: {:?}", checkpoint);
    assert_eq!("mysql-bin.000007", checkpoint.file_name());
    assert_eq!(12345, checkpoint.position());
    assert!(checkpoint.updated_at_ms() > 0);
    assert!(manager.path().exists());

    // 覆盖写
    manager.save(&Checkpoint::new("mysql-bin.000008", 7))?;
    let checkpoint = manager.load()?.expect("checkpoint should exist");
    assert_eq!("mysql-bin.000008", checkpoint.file_name());
    assert_eq!(7, checkpoint.position());

    // 原子写不应残留临时文件
    assert!(!manager.path().with_extension("json.tmp").exists(),
            "temporary file should be renamed away");

    // 转换
    let position: BinlogPosition = checkpoint.to_binlog_position();
    assert_eq!("mysql-bin.000008", position.file_name());
    assert_eq!(7, position.position());

    // 清理
    manager.clear()?;
    assert!(manager.load()?.is_none(), "no checkpoint after clear");

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

/// 位点文件损坏时应被忽略(返回 None)而不是报错
#[test]
fn test_checkpoint_corrupted_file_is_ignored() -> CResult<()> {
    TracingFactory::init_log(true);

    let temp_dir = std::env::temp_dir().join("relay_log_checkpoint_corrupted");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let manager = CheckpointManager::in_dir(&temp_dir);
    std::fs::write(manager.path(), b"{ not a valid json")?;

    assert!(manager.load()?.is_none(), "corrupted checkpoint should be ignored");

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

use tracing::info;
use common::log::tracing_factory::TracingFactory;
use relay_log::relay_log::RelayLog;
use relay_log::storage::relay_log_storage::RelayLogStorage;
use relay_log::storage::segment_file::SegmentFile;
use relay_log::storage::segment_manager::SegmentManager;
use relay_log::storage::storage_config::StorageConfig;

#[test]
pub fn test_segment_file() {
    TracingFactory::init_log(true);
    
    // Create a temporary directory for testing
    let temp_dir = std::env::temp_dir().join("relay_log_test");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let db_table_dir = temp_dir.join("db1#t1");
    std::fs::create_dir_all(&db_table_dir).unwrap();
    
    let segment_path = db_table_dir.join("rlog-1-1-1.log");
    
    // Create an empty file for testing
    std::fs::write(&segment_path, b"").unwrap();
    
    let segment_file = SegmentFile::from_path(segment_path.to_str().unwrap()).unwrap();
    info!("is_segment_file: {:?}", SegmentFile::is_segment_file("rlog-1-1-1.log"));
    info!("version: {:?}", segment_file.version());
    info!("segment_id: {:?}", segment_file.segment_id());
    info!("index: {:?}", segment_file.index());
    
    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
pub fn test_segment_manager() {
    TracingFactory::init_log(true);
    
    // Create a temporary directory for testing
    let temp_dir = std::env::temp_dir().join("relay_log_test_manager");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir(temp_dir.to_str().unwrap().to_string());
    let segment_manager = SegmentManager::new(&storage_config, "db1", "t1").unwrap();
    info!("{:?}", segment_manager);
    
    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
pub fn test_log_storage_append() {
    TracingFactory::init_log(true);
    
    // Create a temporary directory for testing
    let temp_dir = std::env::temp_dir().join("relay_log_test_append");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir(temp_dir.to_str().unwrap().to_string());
    let db_name = "db1";
    let tb_name = "t1";
    let mut log_storage = RelayLogStorage::new(&storage_config, db_name.to_string(), tb_name.to_string()).unwrap();
    for i in 0..203 {
        let mut relay_log = RelayLog::default();
        relay_log.set_database_name(db_name.to_string());
        relay_log.set_table_name(tb_name.to_string());
        relay_log.set_event_log_pos(i as u64);
        relay_log.set_event_name(format!("binlog_{}", i));

        log_storage.append_relay_log(relay_log).unwrap();
    }

    info!("{:?}", &log_storage.segment_manager);
    
    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}

#[test]
pub fn test_log_storage_read() {
    TracingFactory::init_log(true);
    
    // Create a temporary directory for testing
    let temp_dir = std::env::temp_dir().join("relay_log_test_read");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir(temp_dir.to_str().unwrap().to_string());
    let db_name = "db1";
    let tb_name = "t1";
    let mut log_storage = RelayLogStorage::new(&storage_config, db_name.to_string(), tb_name.to_string()).unwrap();

    // First, add some test data
    for i in 0..203 {
        let mut relay_log = RelayLog::default();
        relay_log.set_database_name(db_name.to_string());
        relay_log.set_table_name(tb_name.to_string());
        relay_log.set_event_log_pos(i as u64);
        relay_log.set_event_name(format!("binlog_{}", i));
        log_storage.append_relay_log(relay_log).unwrap();
    }

    let entry = log_storage.get_entry(202).unwrap();
    info!("{:?}", *entry);
    
    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();
}
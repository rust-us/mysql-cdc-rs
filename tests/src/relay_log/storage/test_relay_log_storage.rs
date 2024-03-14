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
    let segment_file = SegmentFile::from_path("/Users/zhangtao/tmp/db1#t1/rlog-1-1-1.log").unwrap();
    info!("is_segment_file: {:?}", SegmentFile::is_segment_file("rlog-1-1-1.log"));
    info!("version: {:?}", segment_file.version());
    info!("segment_id: {:?}", segment_file.segment_id());
    info!("index: {:?}", segment_file.index());
}

#[test]
pub fn test_segment_manager() {
    TracingFactory::init_log(true);
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir("/Users/zhangtao/tmp".to_string());
    let segment_manager = SegmentManager::new(&storage_config, "db1", "t1").unwrap();
    info!("{:?}", segment_manager);
}

#[test]
pub fn test_log_storage_append() {
    TracingFactory::init_log(true);
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir("/Users/zhangtao/tmp".to_string());
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
}

#[test]
pub fn test_log_storage_read() {
    TracingFactory::init_log(true);
    let mut storage_config = StorageConfig::default();
    storage_config.set_relay_log_dir("/Users/zhangtao/tmp".to_string());
    let db_name = "db1";
    let tb_name = "t1";
    let mut log_storage = RelayLogStorage::new(&storage_config, db_name.to_string(), tb_name.to_string()).unwrap();


    let entry = log_storage.get_entry(202).unwrap();
    info!("{:?}", *entry);
}
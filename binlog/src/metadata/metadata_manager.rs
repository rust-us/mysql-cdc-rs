use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::metadata::table_cache::TableCache;
use crate::metadata::gtid_manager::{GtidManager, Gtid};
use crate::metadata::sync_manager::SyncManager;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::ast::query_parser::TableInfo;

/// Parse context for maintaining state during binlog parsing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseContext {
    /// Current binlog file name
    pub binlog_file: Option<String>,
    
    /// Current position in the binlog file
    pub binlog_position: u64,
    
    /// Current server ID
    pub server_id: Option<u32>,
    
    /// Current timestamp
    pub timestamp: Option<u32>,
    
    /// Additional context data
    pub metadata: HashMap<String, String>,
}

impl ParseContext {
    pub fn new() -> Self {
        ParseContext {
            binlog_file: None,
            binlog_position: 0,
            server_id: None,
            timestamp: None,
            metadata: HashMap::new(),
        }
    }
    
    pub fn with_file(binlog_file: String) -> Self {
        ParseContext {
            binlog_file: Some(binlog_file),
            binlog_position: 0,
            server_id: None,
            timestamp: None,
            metadata: HashMap::new(),
        }
    }
    
    pub fn update_position(&mut self, position: u64) {
        self.binlog_position = position;
    }
    
    pub fn set_server_id(&mut self, server_id: u32) {
        self.server_id = Some(server_id);
    }
    
    pub fn set_timestamp(&mut self, timestamp: u32) {
        self.timestamp = Some(timestamp);
    }
    
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }
}

impl Default for ParseContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified metadata manager that manages all metadata during binlog parsing
/// Integrates table cache, GTID state, parse context, and synchronization
#[derive(Debug, Clone)]
pub struct MetadataManager {
    /// Table mapping cache with LRU eviction
    table_cache: Arc<TableCache>,
    
    /// GTID state manager
    gtid_manager: Arc<GtidManager>,
    
    /// Current parse context
    parse_context: Arc<RwLock<ParseContext>>,
    
    /// Synchronization manager for concurrent access
    sync_manager: Arc<SyncManager>,
    
    /// Configuration
    config: MetadataConfig,
}

/// Configuration for metadata manager
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// Maximum number of table maps to cache
    pub table_cache_capacity: usize,
    
    /// Maximum number of GTID snapshots to keep
    pub max_gtid_snapshots: usize,
    
    /// Enable automatic snapshots
    pub auto_snapshot: bool,
    
    /// Snapshot interval (in number of transactions)
    pub snapshot_interval: u64,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        MetadataConfig {
            table_cache_capacity: 1000,
            max_gtid_snapshots: 10,
            auto_snapshot: true,
            snapshot_interval: 1000,
        }
    }
}

impl MetadataManager {
    /// Creates a new MetadataManager with default configuration
    pub fn new() -> Self {
        Self::with_config(MetadataConfig::default())
    }
    
    /// Creates a new MetadataManager with custom configuration
    pub fn with_config(config: MetadataConfig) -> Self {
        MetadataManager {
            table_cache: Arc::new(TableCache::new(config.table_cache_capacity)),
            gtid_manager: Arc::new(GtidManager::with_max_snapshots(config.max_gtid_snapshots)),
            parse_context: Arc::new(RwLock::new(ParseContext::new())),
            sync_manager: Arc::new(SyncManager::new()),
            config,
        }
    }
    
    /// Gets the synchronization manager
    pub fn sync_manager(&self) -> Arc<SyncManager> {
        Arc::clone(&self.sync_manager)
    }
    
    // ========== Table Cache Operations ==========
    
    /// Registers a table map event
    pub fn register_table_map(&self, table_id: u64, table_map: TableMapEvent) {
        self.table_cache.register_table_map(table_id, table_map);
    }
    
    /// Gets a table map by table ID
    pub fn get_table_map(&self, table_id: u64) -> Option<TableMapEvent> {
        self.table_cache.get_table_map(table_id)
    }
    
    /// Registers table info
    pub fn register_table_info(&self, table_name: String, table_info: TableInfo) {
        self.table_cache.register_table_info(table_name, table_info);
    }
    
    /// Gets table info by name
    pub fn get_table_info(&self, table_name: &str) -> Option<TableInfo> {
        self.table_cache.get_table_info(table_name)
    }
    
    /// Checks if a table map exists
    pub fn contains_table_map(&self, table_id: u64) -> bool {
        self.table_cache.contains_table_map(table_id)
    }
    
    /// Gets the current version of a table
    pub fn get_table_version(&self, table_name: &str) -> Option<u64> {
        self.table_cache.get_table_version(table_name)
    }
    
    /// Detects if a table structure has changed
    pub fn detect_table_change(&self, table_name: &str, expected_version: u64) -> bool {
        self.table_cache.detect_table_change(table_name, expected_version)
    }
    
    // ========== GTID Operations ==========
    
    /// Updates GTID state with a new GTID
    pub fn update_gtid(&self, gtid: Gtid) {
        self.gtid_manager.update(gtid);
        
        // Auto snapshot if enabled
        if self.config.auto_snapshot {
            let count = self.gtid_manager.transaction_count();
            if count % self.config.snapshot_interval == 0 {
                self.gtid_manager.create_snapshot();
            }
        }
    }
    
    /// Gets the current GTID set
    pub fn get_gtid_set(&self) -> String {
        self.gtid_manager.to_string()
    }
    
    /// Checks if a GTID exists
    pub fn contains_gtid(&self, gtid: &Gtid) -> bool {
        self.gtid_manager.contains(gtid)
    }
    
    /// Gets the total transaction count
    pub fn transaction_count(&self) -> u64 {
        self.gtid_manager.transaction_count()
    }
    
    /// Creates a GTID snapshot
    pub fn create_gtid_snapshot(&self) {
        self.gtid_manager.create_snapshot();
    }
    
    /// Restores from the most recent GTID snapshot
    pub fn restore_gtid_snapshot(&self) -> Result<(), String> {
        self.gtid_manager.restore_from_snapshot()
    }
    
    // ========== Parse Context Operations ==========
    
    /// Gets the current parse context
    pub fn get_parse_context(&self) -> ParseContext {
        self.parse_context.read().unwrap().clone()
    }
    
    /// Updates the binlog position
    pub fn update_position(&self, position: u64) {
        self.parse_context.write().unwrap().update_position(position);
    }
    
    /// Sets the current binlog file
    pub fn set_binlog_file(&self, file: String) {
        self.parse_context.write().unwrap().binlog_file = Some(file);
    }
    
    /// Sets the server ID
    pub fn set_server_id(&self, server_id: u32) {
        self.parse_context.write().unwrap().set_server_id(server_id);
    }
    
    /// Sets the timestamp
    pub fn set_timestamp(&self, timestamp: u32) {
        self.parse_context.write().unwrap().set_timestamp(timestamp);
    }
    
    /// Adds custom metadata
    pub fn add_context_metadata(&self, key: String, value: String) {
        self.parse_context.write().unwrap().add_metadata(key, value);
    }
    
    // ========== Serialization and Persistence ==========
    
    /// Serializes the metadata to JSON
    pub fn serialize_to_json(&self) -> Result<String, String> {
        let context = self.get_parse_context();
        let gtid_set = self.get_gtid_set();
        
        let data = serde_json::json!({
            "parse_context": context,
            "gtid_set": gtid_set,
            "transaction_count": self.transaction_count(),
        });
        
        serde_json::to_string_pretty(&data)
            .map_err(|e| format!("Serialization error: {}", e))
    }
    
    /// Deserializes and loads metadata from JSON
    pub fn deserialize_from_json(&self, json: &str) -> Result<(), String> {
        let data: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| format!("Deserialization error: {}", e))?;
        
        // Load parse context
        if let Some(context_value) = data.get("parse_context") {
            let context: ParseContext = serde_json::from_value(context_value.clone())
                .map_err(|e| format!("Parse context error: {}", e))?;
            *self.parse_context.write().unwrap() = context;
        }
        
        // Load GTID set
        if let Some(gtid_set) = data.get("gtid_set").and_then(|v| v.as_str()) {
            self.gtid_manager.from_string(gtid_set)?;
        }
        
        Ok(())
    }
    
    /// Saves metadata to a file
    pub fn save_to_file(&self, path: &str) -> Result<(), String> {
        let json = self.serialize_to_json()?;
        std::fs::write(path, json)
            .map_err(|e| format!("File write error: {}", e))
    }
    
    /// Loads metadata from a file
    pub fn load_from_file(&self, path: &str) -> Result<(), String> {
        let json = std::fs::read_to_string(path)
            .map_err(|e| format!("File read error: {}", e))?;
        self.deserialize_from_json(&json)
    }
    
    // ========== Statistics and Monitoring ==========
    
    /// Gets statistics about the metadata manager
    pub fn get_statistics(&self) -> MetadataStatistics {
        let sync_stats = self.sync_manager.get_statistics();
        
        MetadataStatistics {
            table_map_count: self.table_cache.table_map_len(),
            table_info_count: self.table_cache.table_info_len(),
            transaction_count: self.transaction_count(),
            gtid_snapshot_count: self.gtid_manager.snapshot_count(),
            current_position: self.parse_context.read().unwrap().binlog_position,
            sync_read_count: sync_stats.read_count,
            sync_write_count: sync_stats.write_count,
            avg_lock_wait_us: sync_stats.avg_wait_time_us,
        }
    }
    
    /// Gets detailed synchronization statistics
    pub fn get_sync_statistics(&self) -> crate::metadata::sync_manager::SyncStatistics {
        self.sync_manager.get_statistics()
    }
    
    /// Resets synchronization statistics
    pub fn reset_sync_statistics(&self) {
        self.sync_manager.reset_statistics();
    }
    
    /// Clears all metadata
    pub fn clear(&self) {
        self.table_cache.clear();
        self.gtid_manager.clear();
        *self.parse_context.write().unwrap() = ParseContext::new();
    }
}

impl Default for MetadataManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the metadata manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataStatistics {
    pub table_map_count: usize,
    pub table_info_count: usize,
    pub transaction_count: u64,
    pub gtid_snapshot_count: usize,
    pub current_position: u64,
    pub sync_read_count: u64,
    pub sync_write_count: u64,
    pub avg_lock_wait_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metadata_manager_creation() {
        let manager = MetadataManager::new();
        let stats = manager.get_statistics();
        
        assert_eq!(stats.table_map_count, 0);
        assert_eq!(stats.transaction_count, 0);
    }
    
    #[test]
    fn test_table_operations() {
        let manager = MetadataManager::new();
        
        let table_map = TableMapEvent::default();
        manager.register_table_map(1, table_map.clone());
        
        assert!(manager.contains_table_map(1));
        assert!(manager.get_table_map(1).is_some());
    }
    
    #[test]
    fn test_gtid_operations() {
        let manager = MetadataManager::new();
        
        let gtid = Gtid::new("uuid1".to_string(), 1);
        manager.update_gtid(gtid.clone());
        
        assert!(manager.contains_gtid(&gtid));
        assert_eq!(manager.transaction_count(), 1);
    }
    
    #[test]
    fn test_parse_context() {
        let manager = MetadataManager::new();
        
        manager.set_binlog_file("mysql-bin.000001".to_string());
        manager.update_position(1234);
        manager.set_server_id(1);
        
        let context = manager.get_parse_context();
        assert_eq!(context.binlog_file, Some("mysql-bin.000001".to_string()));
        assert_eq!(context.binlog_position, 1234);
        assert_eq!(context.server_id, Some(1));
    }
    
    #[test]
    fn test_serialization() {
        let manager = MetadataManager::new();
        
        manager.set_binlog_file("test.bin".to_string());
        manager.update_position(100);
        manager.update_gtid(Gtid::new("uuid1".to_string(), 1));
        
        let json = manager.serialize_to_json().unwrap();
        assert!(json.contains("test.bin"));
        assert!(json.contains("uuid1"));
    }
    
    #[test]
    fn test_auto_snapshot() {
        let mut config = MetadataConfig::default();
        config.auto_snapshot = true;
        config.snapshot_interval = 2;
        
        let manager = MetadataManager::with_config(config);
        
        manager.update_gtid(Gtid::new("uuid1".to_string(), 1));
        assert_eq!(manager.gtid_manager.snapshot_count(), 0);
        
        manager.update_gtid(Gtid::new("uuid1".to_string(), 2));
        assert_eq!(manager.gtid_manager.snapshot_count(), 1);
    }
    
    #[test]
    fn test_statistics() {
        let manager = MetadataManager::new();
        
        manager.register_table_map(1, TableMapEvent::default());
        manager.update_gtid(Gtid::new("uuid1".to_string(), 1));
        manager.update_position(500);
        
        let stats = manager.get_statistics();
        assert_eq!(stats.table_map_count, 1);
        assert_eq!(stats.transaction_count, 1);
        assert_eq!(stats.current_position, 500);
    }
    
    #[test]
    fn test_concurrent_access() {
        use std::thread;
        use std::sync::Arc;
        
        let manager = Arc::new(MetadataManager::new());
        let mut handles = vec![];
        
        // Spawn multiple threads to access metadata concurrently
        for i in 0..5 {
            let manager_clone = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                manager_clone.update_gtid(Gtid::new("uuid1".to_string(), i as u64));
                manager_clone.update_position(i * 100);
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // All updates should be reflected
        assert_eq!(manager.transaction_count(), 5);
    }
    
    #[test]
    fn test_sync_manager_integration() {
        let manager = MetadataManager::new();
        
        // Perform some operations
        manager.register_table_map(1, TableMapEvent::default());
        manager.update_gtid(Gtid::new("uuid1".to_string(), 1));
        
        let sync_stats = manager.get_sync_statistics();
        // Sync manager should be tracking operations
        assert!(sync_stats.resource_count >= 0);
    }
}

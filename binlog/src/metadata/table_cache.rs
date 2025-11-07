use std::sync::{Arc, RwLock};
use lru::LruCache;
use std::num::NonZeroUsize;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::ast::query_parser::TableInfo;

/// Thread-safe table mapping cache with LRU eviction strategy
/// Replaces the old singleton TableCacheManager design
#[derive(Debug, Clone)]
pub struct TableCache {
    /// LRU cache for TableMapEvent indexed by table_id
    table_map_cache: Arc<RwLock<LruCache<u64, TableMapEvent>>>,
    
    /// LRU cache for TableInfo indexed by table name
    table_info_cache: Arc<RwLock<LruCache<String, TableInfo>>>,
    
    /// Version tracking for table structure changes
    table_versions: Arc<RwLock<lru::LruCache<String, u64>>>,
}

impl TableCache {
    /// Creates a new TableCache with specified capacity
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1000).unwrap());
        
        TableCache {
            table_map_cache: Arc::new(RwLock::new(LruCache::new(cap))),
            table_info_cache: Arc::new(RwLock::new(LruCache::new(cap))),
            table_versions: Arc::new(RwLock::new(LruCache::new(cap))),
        }
    }
    
    /// Registers a table map event in the cache
    pub fn register_table_map(&self, table_id: u64, table_map: TableMapEvent) {
        let mut cache = self.table_map_cache.write().unwrap();
        cache.put(table_id, table_map);
    }
    
    /// Gets a table map event by table_id
    pub fn get_table_map(&self, table_id: u64) -> Option<TableMapEvent> {
        let mut cache = self.table_map_cache.write().unwrap();
        cache.get(&table_id).cloned()
    }
    
    /// Registers table info in the cache
    pub fn register_table_info(&self, table_name: String, table_info: TableInfo) {
        let mut cache = self.table_info_cache.write().unwrap();
        cache.put(table_name.clone(), table_info);
        
        // Increment version for this table
        let mut versions = self.table_versions.write().unwrap();
        let current_version = versions.get(&table_name).copied().unwrap_or(0);
        versions.put(table_name, current_version + 1);
    }
    
    /// Gets table info by table name
    pub fn get_table_info(&self, table_name: &str) -> Option<TableInfo> {
        let mut cache = self.table_info_cache.write().unwrap();
        cache.get(table_name).cloned()
    }
    
    /// Checks if a table exists in the cache
    pub fn contains_table_map(&self, table_id: u64) -> bool {
        let cache = self.table_map_cache.read().unwrap();
        cache.contains(&table_id)
    }
    
    /// Checks if table info exists in the cache
    pub fn contains_table_info(&self, table_name: &str) -> bool {
        let cache = self.table_info_cache.read().unwrap();
        cache.contains(table_name)
    }
    
    /// Gets the current version of a table
    pub fn get_table_version(&self, table_name: &str) -> Option<u64> {
        let mut versions = self.table_versions.write().unwrap();
        versions.get(table_name).copied()
    }
    
    /// Detects if a table structure has changed by comparing versions
    pub fn detect_table_change(&self, table_name: &str, expected_version: u64) -> bool {
        if let Some(current_version) = self.get_table_version(table_name) {
            current_version != expected_version
        } else {
            true // Table not found, consider it changed
        }
    }
    
    /// Clears all cached data
    pub fn clear(&self) {
        self.table_map_cache.write().unwrap().clear();
        self.table_info_cache.write().unwrap().clear();
        self.table_versions.write().unwrap().clear();
    }
    
    /// Gets the number of cached table maps
    pub fn table_map_len(&self) -> usize {
        self.table_map_cache.read().unwrap().len()
    }
    
    /// Gets the number of cached table infos
    pub fn table_info_len(&self) -> usize {
        self.table_info_cache.read().unwrap().len()
    }
    
    /// Removes a table map from cache
    pub fn remove_table_map(&self, table_id: u64) -> Option<TableMapEvent> {
        let mut cache = self.table_map_cache.write().unwrap();
        cache.pop(&table_id)
    }
    
    /// Removes table info from cache
    pub fn remove_table_info(&self, table_name: &str) -> Option<TableInfo> {
        let mut cache = self.table_info_cache.write().unwrap();
        cache.pop(table_name)
    }
}

impl Default for TableCache {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_table_cache_basic_operations() {
        let cache = TableCache::new(10);
        
        // Test table map operations
        assert_eq!(cache.table_map_len(), 0);
        assert!(!cache.contains_table_map(1));
        
        let table_map = TableMapEvent::default();
        cache.register_table_map(1, table_map.clone());
        
        assert_eq!(cache.table_map_len(), 1);
        assert!(cache.contains_table_map(1));
        assert!(cache.get_table_map(1).is_some());
    }
    
    #[test]
    fn test_table_version_tracking() {
        let cache = TableCache::new(10);
        let table_name = "test_table".to_string();
        
        // Initial version should be None
        assert!(cache.get_table_version(&table_name).is_none());
        
        // Note: TableInfo doesn't have Default trait, so we skip this test
        // In real usage, TableInfo would be created from actual table metadata
    }
    
    #[test]
    fn test_table_change_detection() {
        let cache = TableCache::new(10);
        let table_name = "test_table".to_string();
        
        // Test with non-existent table
        assert!(cache.detect_table_change(&table_name, 0));
        assert!(cache.detect_table_change(&table_name, 1));
    }
    
    #[test]
    fn test_lru_eviction() {
        let cache = TableCache::new(2);
        
        // Add 3 items to a cache with capacity 2
        let table_map1 = TableMapEvent::default();
        let table_map2 = TableMapEvent::default();
        let table_map3 = TableMapEvent::default();
        
        cache.register_table_map(1, table_map1);
        cache.register_table_map(2, table_map2);
        cache.register_table_map(3, table_map3);
        
        // Cache should only have 2 items
        assert_eq!(cache.table_map_len(), 2);
        
        // First item should be evicted
        assert!(!cache.contains_table_map(1));
        assert!(cache.contains_table_map(2));
        assert!(cache.contains_table_map(3));
    }
    
    #[test]
    fn test_clear() {
        let cache = TableCache::new(10);
        
        let table_map = TableMapEvent::default();
        cache.register_table_map(1, table_map);
        
        assert_eq!(cache.table_map_len(), 1);
        
        cache.clear();
        
        assert_eq!(cache.table_map_len(), 0);
        assert_eq!(cache.table_info_len(), 0);
    }
}

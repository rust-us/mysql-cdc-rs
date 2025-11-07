use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};

/// Represents a single GTID (Global Transaction Identifier)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Gtid {
    /// Server UUID
    pub server_uuid: String,
    /// Transaction ID
    pub transaction_id: u64,
}

impl Gtid {
    pub fn new(server_uuid: String, transaction_id: u64) -> Self {
        Gtid {
            server_uuid,
            transaction_id,
        }
    }
    
    /// Parses a GTID from string format: "uuid:transaction_id"
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid GTID format: {}", s));
        }
        
        let server_uuid = parts[0].to_string();
        let transaction_id = parts[1].parse::<u64>()
            .map_err(|e| format!("Invalid transaction ID: {}", e))?;
        
        Ok(Gtid::new(server_uuid, transaction_id))
    }
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.server_uuid, self.transaction_id)
    }
}

/// Represents a range of transaction IDs for a server
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GtidInterval {
    pub start: u64,
    pub end: u64, // inclusive
}

impl GtidInterval {
    pub fn new(start: u64, end: u64) -> Self {
        GtidInterval { start, end }
    }
    
    pub fn contains(&self, transaction_id: u64) -> bool {
        transaction_id >= self.start && transaction_id <= self.end
    }
    
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// Represents a set of GTIDs with compression support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtidSet {
    /// Map of server UUID to sorted intervals
    intervals: BTreeMap<String, Vec<GtidInterval>>,
}

impl GtidSet {
    pub fn new() -> Self {
        GtidSet {
            intervals: BTreeMap::new(),
        }
    }
    
    /// Adds a GTID to the set
    pub fn add(&mut self, gtid: Gtid) {
        let intervals = self.intervals.entry(gtid.server_uuid.clone())
            .or_insert_with(Vec::new);
        
        // Try to merge with existing intervals
        let mut merged = false;
        for interval in intervals.iter_mut() {
            if gtid.transaction_id == interval.end + 1 {
                // Extend the end of this interval
                interval.end = gtid.transaction_id;
                merged = true;
                break;
            } else if gtid.transaction_id + 1 == interval.start {
                // Extend the start of this interval
                interval.start = gtid.transaction_id;
                merged = true;
                break;
            } else if interval.contains(gtid.transaction_id) {
                // Already in the set
                merged = true;
                break;
            }
        }
        
        if !merged {
            // Add as new interval
            intervals.push(GtidInterval::new(gtid.transaction_id, gtid.transaction_id));
            intervals.sort_by_key(|i| i.start);
        }
        
        // Always try to merge adjacent intervals after adding
        self.merge_intervals(&gtid.server_uuid);
    }
    
    /// Merges adjacent intervals for a server
    fn merge_intervals(&mut self, server_uuid: &str) {
        if let Some(intervals) = self.intervals.get_mut(server_uuid) {
            if intervals.len() <= 1 {
                return;
            }
            
            let mut merged = Vec::new();
            let mut current = intervals[0].clone();
            
            for interval in intervals.iter().skip(1) {
                if interval.start <= current.end + 1 {
                    // Merge with current
                    current.end = current.end.max(interval.end);
                } else {
                    // Save current and start new
                    merged.push(current);
                    current = interval.clone();
                }
            }
            merged.push(current);
            
            *intervals = merged;
        }
    }
    
    /// Checks if a GTID is in the set
    pub fn contains(&self, gtid: &Gtid) -> bool {
        if let Some(intervals) = self.intervals.get(&gtid.server_uuid) {
            intervals.iter().any(|i| i.contains(gtid.transaction_id))
        } else {
            false
        }
    }
    
    /// Gets the total number of transactions in the set
    pub fn count(&self) -> u64 {
        self.intervals.values()
            .flat_map(|intervals| intervals.iter())
            .map(|i| i.len())
            .sum()
    }
    
    /// Parses a GTID set from string format
    /// Format: "uuid:1-5:10-15,uuid2:1-3"
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut gtid_set = GtidSet::new();
        
        if s.is_empty() {
            return Ok(gtid_set);
        }
        
        for server_part in s.split(',') {
            let parts: Vec<&str> = server_part.split(':').collect();
            if parts.is_empty() {
                continue;
            }
            
            let server_uuid = parts[0].to_string();
            
            for range_str in parts.iter().skip(1) {
                if range_str.contains('-') {
                    let range_parts: Vec<&str> = range_str.split('-').collect();
                    if range_parts.len() == 2 {
                        let start = range_parts[0].parse::<u64>()
                            .map_err(|e| format!("Invalid start: {}", e))?;
                        let end = range_parts[1].parse::<u64>()
                            .map_err(|e| format!("Invalid end: {}", e))?;
                        
                        let intervals = gtid_set.intervals.entry(server_uuid.clone())
                            .or_insert_with(Vec::new);
                        intervals.push(GtidInterval::new(start, end));
                    }
                } else {
                    let transaction_id = range_str.parse::<u64>()
                        .map_err(|e| format!("Invalid transaction ID: {}", e))?;
                    gtid_set.add(Gtid::new(server_uuid.clone(), transaction_id));
                }
            }
        }
        
        Ok(gtid_set)
    }
    
    /// Converts the GTID set to string format
    pub fn to_string(&self) -> String {
        let mut parts = Vec::new();
        
        for (server_uuid, intervals) in &self.intervals {
            let mut interval_strs = Vec::new();
            for interval in intervals {
                if interval.start == interval.end {
                    interval_strs.push(format!("{}", interval.start));
                } else {
                    interval_strs.push(format!("{}-{}", interval.start, interval.end));
                }
            }
            
            if !interval_strs.is_empty() {
                parts.push(format!("{}:{}", server_uuid, interval_strs.join(":")));
            }
        }
        
        parts.join(",")
    }
    
    /// Clears all GTIDs from the set
    pub fn clear(&mut self) {
        self.intervals.clear();
    }
}

impl Default for GtidSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe GTID state manager with performance optimizations
#[derive(Debug, Clone)]
pub struct GtidManager {
    /// Current GTID set with compressed storage
    gtid_set: Arc<RwLock<GtidSet>>,
    
    /// Snapshots for recovery
    snapshots: Arc<RwLock<Vec<GtidSet>>>,
    
    /// Maximum number of snapshots to keep
    max_snapshots: usize,
    
    /// Incremental updates buffer for batch processing
    update_buffer: Arc<RwLock<Vec<Gtid>>>,
    
    /// Buffer size threshold for flushing
    buffer_threshold: usize,
}

impl GtidManager {
    pub fn new() -> Self {
        GtidManager {
            gtid_set: Arc::new(RwLock::new(GtidSet::new())),
            snapshots: Arc::new(RwLock::new(Vec::new())),
            max_snapshots: 10,
            update_buffer: Arc::new(RwLock::new(Vec::new())),
            buffer_threshold: 100,
        }
    }
    
    pub fn with_max_snapshots(max_snapshots: usize) -> Self {
        GtidManager {
            gtid_set: Arc::new(RwLock::new(GtidSet::new())),
            snapshots: Arc::new(RwLock::new(Vec::new())),
            max_snapshots,
            update_buffer: Arc::new(RwLock::new(Vec::new())),
            buffer_threshold: 100,
        }
    }
    
    pub fn with_config(max_snapshots: usize, buffer_threshold: usize) -> Self {
        GtidManager {
            gtid_set: Arc::new(RwLock::new(GtidSet::new())),
            snapshots: Arc::new(RwLock::new(Vec::new())),
            max_snapshots,
            update_buffer: Arc::new(RwLock::new(Vec::new())),
            buffer_threshold,
        }
    }
    
    /// Updates the GTID set with a new GTID (immediate mode)
    pub fn update(&self, gtid: Gtid) {
        let mut set = self.gtid_set.write().unwrap();
        set.add(gtid);
    }
    
    /// Adds a GTID to the incremental update buffer
    /// More efficient for batch processing
    pub fn update_incremental(&self, gtid: Gtid) {
        let mut buffer = self.update_buffer.write().unwrap();
        buffer.push(gtid);
        
        // Auto-flush if buffer is full
        if buffer.len() >= self.buffer_threshold {
            drop(buffer); // Release lock before flushing
            self.flush_updates();
        }
    }
    
    /// Flushes all buffered updates to the GTID set
    pub fn flush_updates(&self) {
        let mut buffer = self.update_buffer.write().unwrap();
        if buffer.is_empty() {
            return;
        }
        
        let mut set = self.gtid_set.write().unwrap();
        for gtid in buffer.drain(..) {
            set.add(gtid);
        }
    }
    
    /// Gets the number of buffered updates
    pub fn buffered_count(&self) -> usize {
        self.update_buffer.read().unwrap().len()
    }
    
    /// Gets the current GTID set
    pub fn get_gtid_set(&self) -> GtidSet {
        self.gtid_set.read().unwrap().clone()
    }
    
    /// Checks if a GTID exists in the set
    pub fn contains(&self, gtid: &Gtid) -> bool {
        self.gtid_set.read().unwrap().contains(gtid)
    }
    
    /// Gets the total transaction count
    pub fn transaction_count(&self) -> u64 {
        self.gtid_set.read().unwrap().count()
    }
    
    /// Creates a snapshot of the current GTID state
    pub fn create_snapshot(&self) {
        let current_set = self.gtid_set.read().unwrap().clone();
        let mut snapshots = self.snapshots.write().unwrap();
        
        snapshots.push(current_set);
        
        // Keep only the last N snapshots
        if snapshots.len() > self.max_snapshots {
            snapshots.remove(0);
        }
    }
    
    /// Restores from the most recent snapshot
    pub fn restore_from_snapshot(&self) -> Result<(), String> {
        let mut snapshots = self.snapshots.write().unwrap();
        
        if let Some(snapshot) = snapshots.pop() {
            let mut set = self.gtid_set.write().unwrap();
            *set = snapshot;
            Ok(())
        } else {
            Err("No snapshots available".to_string())
        }
    }
    
    /// Gets the number of available snapshots
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.read().unwrap().len()
    }
    
    /// Clears all GTID state
    pub fn clear(&self) {
        self.gtid_set.write().unwrap().clear();
        self.snapshots.write().unwrap().clear();
        self.update_buffer.write().unwrap().clear();
    }
    
    /// Compresses the GTID set by merging adjacent intervals
    /// This is automatically done during add operations, but can be called manually
    pub fn compress(&self) {
        let mut set = self.gtid_set.write().unwrap();
        for server_uuid in set.intervals.keys().cloned().collect::<Vec<_>>() {
            set.merge_intervals(&server_uuid);
        }
    }
    
    /// Gets compression statistics
    pub fn get_compression_stats(&self) -> CompressionStats {
        let set = self.gtid_set.read().unwrap();
        let total_transactions = set.count();
        let total_intervals: usize = set.intervals.values()
            .map(|intervals| intervals.len())
            .sum();
        
        let compression_ratio = if total_intervals > 0 {
            total_transactions as f64 / total_intervals as f64
        } else {
            0.0
        };
        
        CompressionStats {
            total_transactions,
            total_intervals,
            compression_ratio,
            server_count: set.intervals.len(),
        }
    }
    
    /// Serializes the GTID set to string
    pub fn to_string(&self) -> String {
        self.gtid_set.read().unwrap().to_string()
    }
    
    /// Parses and loads a GTID set from string
    pub fn from_string(&self, s: &str) -> Result<(), String> {
        let gtid_set = GtidSet::parse(s)?;
        let mut set = self.gtid_set.write().unwrap();
        *set = gtid_set;
        Ok(())
    }
}

impl Default for GtidManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about GTID compression
#[derive(Debug, Clone)]
pub struct CompressionStats {
    /// Total number of transactions
    pub total_transactions: u64,
    
    /// Total number of intervals (after compression)
    pub total_intervals: usize,
    
    /// Compression ratio (transactions per interval)
    pub compression_ratio: f64,
    
    /// Number of different servers
    pub server_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_gtid_parse() {
        let gtid = Gtid::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1").unwrap();
        assert_eq!(gtid.server_uuid, "3E11FA47-71CA-11E1-9E33-C80AA9429562");
        assert_eq!(gtid.transaction_id, 1);
    }
    
    #[test]
    fn test_gtid_set_add() {
        let mut set = GtidSet::new();
        
        set.add(Gtid::new("uuid1".to_string(), 1));
        set.add(Gtid::new("uuid1".to_string(), 2));
        set.add(Gtid::new("uuid1".to_string(), 3));
        
        assert_eq!(set.count(), 3);
        assert!(set.contains(&Gtid::new("uuid1".to_string(), 2)));
    }
    
    #[test]
    fn test_gtid_set_merge() {
        let mut set = GtidSet::new();
        
        // Add non-adjacent transactions
        set.add(Gtid::new("uuid1".to_string(), 1));
        set.add(Gtid::new("uuid1".to_string(), 3));
        set.add(Gtid::new("uuid1".to_string(), 5));
        
        // Fill the gap
        set.add(Gtid::new("uuid1".to_string(), 2));
        set.add(Gtid::new("uuid1".to_string(), 4));
        
        // Should be merged into one interval
        assert_eq!(set.count(), 5);
        let intervals = &set.intervals["uuid1"];
        assert_eq!(intervals.len(), 1);
        assert_eq!(intervals[0].start, 1);
        assert_eq!(intervals[0].end, 5);
    }
    
    #[test]
    fn test_gtid_set_parse() {
        let set = GtidSet::parse("uuid1:1-5:10-15,uuid2:1-3").unwrap();
        
        assert_eq!(set.count(), 14); // 5 + 6 + 3
        assert!(set.contains(&Gtid::new("uuid1".to_string(), 3)));
        assert!(set.contains(&Gtid::new("uuid1".to_string(), 12)));
        assert!(set.contains(&Gtid::new("uuid2".to_string(), 2)));
        assert!(!set.contains(&Gtid::new("uuid1".to_string(), 7)));
    }
    
    #[test]
    fn test_gtid_set_to_string() {
        let mut set = GtidSet::new();
        set.add(Gtid::new("uuid1".to_string(), 1));
        set.add(Gtid::new("uuid1".to_string(), 2));
        set.add(Gtid::new("uuid1".to_string(), 3));
        
        let s = set.to_string();
        assert_eq!(s, "uuid1:1-3");
    }
    
    #[test]
    fn test_gtid_manager_update() {
        let manager = GtidManager::new();
        
        manager.update(Gtid::new("uuid1".to_string(), 1));
        manager.update(Gtid::new("uuid1".to_string(), 2));
        
        assert_eq!(manager.transaction_count(), 2);
        assert!(manager.contains(&Gtid::new("uuid1".to_string(), 1)));
    }
    
    #[test]
    fn test_gtid_manager_snapshot() {
        let manager = GtidManager::new();
        
        manager.update(Gtid::new("uuid1".to_string(), 1));
        manager.create_snapshot();
        
        manager.update(Gtid::new("uuid1".to_string(), 2));
        assert_eq!(manager.transaction_count(), 2);
        
        manager.restore_from_snapshot().unwrap();
        assert_eq!(manager.transaction_count(), 1);
    }
    
    #[test]
    fn test_incremental_updates() {
        let manager = GtidManager::with_config(10, 5);
        
        // Add updates to buffer
        for i in 1..=3 {
            manager.update_incremental(Gtid::new("uuid1".to_string(), i));
        }
        
        assert_eq!(manager.buffered_count(), 3);
        assert_eq!(manager.transaction_count(), 0); // Not flushed yet
        
        manager.flush_updates();
        assert_eq!(manager.buffered_count(), 0);
        assert_eq!(manager.transaction_count(), 3);
    }
    
    #[test]
    fn test_auto_flush() {
        let manager = GtidManager::with_config(10, 3);
        
        // Add 3 updates - should auto-flush
        for i in 1..=3 {
            manager.update_incremental(Gtid::new("uuid1".to_string(), i));
        }
        
        // Buffer should be empty after auto-flush
        assert_eq!(manager.buffered_count(), 0);
        assert_eq!(manager.transaction_count(), 3);
    }
    
    #[test]
    fn test_compression_stats() {
        let manager = GtidManager::new();
        
        // Add consecutive transactions
        for i in 1..=10 {
            manager.update(Gtid::new("uuid1".to_string(), i));
        }
        
        let stats = manager.get_compression_stats();
        assert_eq!(stats.total_transactions, 10);
        assert_eq!(stats.total_intervals, 1); // Should be compressed to 1 interval
        assert_eq!(stats.compression_ratio, 10.0);
        assert_eq!(stats.server_count, 1);
    }
}

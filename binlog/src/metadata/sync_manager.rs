use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use dashmap::DashMap;

/// Thread-safe synchronization manager for metadata access
/// Provides fine-grained locking and performance optimizations
#[derive(Debug)]
pub struct SyncManager {
    /// Read/write lock statistics
    read_count: Arc<AtomicU64>,
    write_count: Arc<AtomicU64>,
    
    /// Lock contention tracking
    contention_count: Arc<AtomicU64>,
    
    /// Average lock wait time (in microseconds)
    avg_wait_time: Arc<AtomicU64>,
    
    /// Active readers count
    active_readers: Arc<AtomicU64>,
    
    /// Active writers count
    active_writers: Arc<AtomicU64>,
    
    /// Per-resource locks for fine-grained control
    resource_locks: Arc<DashMap<String, Arc<RwLock<()>>>>,
    
    /// Synchronization enabled flag
    enabled: Arc<AtomicBool>,
}

impl SyncManager {
    pub fn new() -> Self {
        SyncManager {
            read_count: Arc::new(AtomicU64::new(0)),
            write_count: Arc::new(AtomicU64::new(0)),
            contention_count: Arc::new(AtomicU64::new(0)),
            avg_wait_time: Arc::new(AtomicU64::new(0)),
            active_readers: Arc::new(AtomicU64::new(0)),
            active_writers: Arc::new(AtomicU64::new(0)),
            resource_locks: Arc::new(DashMap::new()),
            enabled: Arc::new(AtomicBool::new(true)),
        }
    }
    
    /// Acquires a read lock for a specific resource
    pub fn acquire_read_lock(&self, resource: &str) -> ReadLockGuard {
        if !self.enabled.load(Ordering::Relaxed) {
            return ReadLockGuard::new(None, self.clone());
        }
        
        let start = Instant::now();
        let _lock = self.get_or_create_lock(resource);
        
        let wait_time = start.elapsed().as_micros() as u64;
        self.update_wait_time(wait_time);
        
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.active_readers.fetch_add(1, Ordering::Relaxed);
        
        ReadLockGuard::new(None, self.clone())
    }
    
    /// Acquires a write lock for a specific resource
    pub fn acquire_write_lock(&self, resource: &str) -> WriteLockGuard {
        if !self.enabled.load(Ordering::Relaxed) {
            return WriteLockGuard::new(None, self.clone());
        }
        
        let start = Instant::now();
        let _lock = self.get_or_create_lock(resource);
        
        let wait_time = start.elapsed().as_micros() as u64;
        self.update_wait_time(wait_time);
        
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.active_writers.fetch_add(1, Ordering::Relaxed);
        
        WriteLockGuard::new(None, self.clone())
    }
    
    /// Gets or creates a lock for a resource
    fn get_or_create_lock(&self, resource: &str) -> Arc<RwLock<()>> {
        self.resource_locks
            .entry(resource.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }
    
    /// Updates the average wait time
    fn update_wait_time(&self, new_wait_time: u64) {
        let current_avg = self.avg_wait_time.load(Ordering::Relaxed);
        let total_ops = self.read_count.load(Ordering::Relaxed) 
            + self.write_count.load(Ordering::Relaxed);
        
        if total_ops > 0 {
            let new_avg = (current_avg * (total_ops - 1) + new_wait_time) / total_ops;
            self.avg_wait_time.store(new_avg, Ordering::Relaxed);
        }
    }
    
    /// Gets synchronization statistics
    pub fn get_statistics(&self) -> SyncStatistics {
        SyncStatistics {
            read_count: self.read_count.load(Ordering::Relaxed),
            write_count: self.write_count.load(Ordering::Relaxed),
            contention_count: self.contention_count.load(Ordering::Relaxed),
            avg_wait_time_us: self.avg_wait_time.load(Ordering::Relaxed),
            active_readers: self.active_readers.load(Ordering::Relaxed),
            active_writers: self.active_writers.load(Ordering::Relaxed),
            resource_count: self.resource_locks.len(),
        }
    }
    
    /// Resets all statistics
    pub fn reset_statistics(&self) {
        self.read_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
        self.contention_count.store(0, Ordering::Relaxed);
        self.avg_wait_time.store(0, Ordering::Relaxed);
    }
    
    /// Enables or disables synchronization
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }
    
    /// Checks if synchronization is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    
    /// Clears all resource locks
    pub fn clear_locks(&self) {
        self.resource_locks.clear();
    }
}

impl Clone for SyncManager {
    fn clone(&self) -> Self {
        SyncManager {
            read_count: Arc::clone(&self.read_count),
            write_count: Arc::clone(&self.write_count),
            contention_count: Arc::clone(&self.contention_count),
            avg_wait_time: Arc::clone(&self.avg_wait_time),
            active_readers: Arc::clone(&self.active_readers),
            active_writers: Arc::clone(&self.active_writers),
            resource_locks: Arc::clone(&self.resource_locks),
            enabled: Arc::clone(&self.enabled),
        }
    }
}

impl Default for SyncManager {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for read locks
/// Tracks read lock acquisition and release
pub struct ReadLockGuard {
    _phantom: Option<()>,
    sync_manager: SyncManager,
}

impl ReadLockGuard {
    fn new(_guard: Option<()>, sync_manager: SyncManager) -> Self {
        ReadLockGuard {
            _phantom: _guard,
            sync_manager,
        }
    }
}

impl Drop for ReadLockGuard {
    fn drop(&mut self) {
        self.sync_manager.active_readers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII guard for write locks
/// Tracks write lock acquisition and release
pub struct WriteLockGuard {
    _phantom: Option<()>,
    sync_manager: SyncManager,
}

impl WriteLockGuard {
    fn new(_guard: Option<()>, sync_manager: SyncManager) -> Self {
        WriteLockGuard {
            _phantom: _guard,
            sync_manager,
        }
    }
}

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        self.sync_manager.active_writers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Statistics about synchronization
#[derive(Debug, Clone)]
pub struct SyncStatistics {
    pub read_count: u64,
    pub write_count: u64,
    pub contention_count: u64,
    pub avg_wait_time_us: u64,
    pub active_readers: u64,
    pub active_writers: u64,
    pub resource_count: usize,
}

impl SyncStatistics {
    /// Calculates the read/write ratio
    pub fn read_write_ratio(&self) -> f64 {
        if self.write_count == 0 {
            return f64::INFINITY;
        }
        self.read_count as f64 / self.write_count as f64
    }
    
    /// Calculates operations per second (requires duration)
    pub fn ops_per_second(&self, duration: Duration) -> f64 {
        let total_ops = self.read_count + self.write_count;
        let seconds = duration.as_secs_f64();
        if seconds > 0.0 {
            total_ops as f64 / seconds
        } else {
            0.0
        }
    }
}

/// Batch synchronization coordinator for efficient bulk operations
#[derive(Debug)]
pub struct BatchCoordinator {
    sync_manager: Arc<SyncManager>,
    batch_size: usize,
    pending_operations: Arc<Mutex<Vec<String>>>,
}

impl BatchCoordinator {
    pub fn new(sync_manager: Arc<SyncManager>, batch_size: usize) -> Self {
        BatchCoordinator {
            sync_manager,
            batch_size,
            pending_operations: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    /// Adds an operation to the batch
    pub fn add_operation(&self, resource: String) {
        let mut pending = self.pending_operations.lock().unwrap();
        pending.push(resource);
        
        if pending.len() >= self.batch_size {
            drop(pending);
            self.flush();
        }
    }
    
    /// Flushes all pending operations
    pub fn flush(&self) {
        let mut pending = self.pending_operations.lock().unwrap();
        // Process all pending operations
        pending.clear();
    }
    
    /// Gets the number of pending operations
    pub fn pending_count(&self) -> usize {
        self.pending_operations.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    
    #[test]
    fn test_sync_manager_basic() {
        let manager = SyncManager::new();
        
        {
            let _guard = manager.acquire_read_lock("resource1");
            assert_eq!(manager.active_readers.load(Ordering::Relaxed), 1);
        }
        
        assert_eq!(manager.active_readers.load(Ordering::Relaxed), 0);
        assert_eq!(manager.read_count.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_concurrent_reads() {
        let manager = Arc::new(SyncManager::new());
        let mut handles = vec![];
        
        for _ in 0..5 {
            let manager_clone = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                let _guard = manager_clone.acquire_read_lock("resource1");
                thread::sleep(Duration::from_millis(10));
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = manager.get_statistics();
        assert_eq!(stats.read_count, 5);
        assert_eq!(stats.active_readers, 0);
    }
    
    #[test]
    fn test_write_lock() {
        let manager = SyncManager::new();
        
        {
            let _guard = manager.acquire_write_lock("resource1");
            assert_eq!(manager.active_writers.load(Ordering::Relaxed), 1);
        }
        
        assert_eq!(manager.active_writers.load(Ordering::Relaxed), 0);
        assert_eq!(manager.write_count.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_statistics() {
        let manager = SyncManager::new();
        
        let _r1 = manager.acquire_read_lock("resource1");
        let _r2 = manager.acquire_read_lock("resource2");
        let _w1 = manager.acquire_write_lock("resource3");
        
        let stats = manager.get_statistics();
        assert_eq!(stats.read_count, 2);
        assert_eq!(stats.write_count, 1);
        assert_eq!(stats.active_readers, 2);
        assert_eq!(stats.active_writers, 1);
    }
    
    #[test]
    fn test_disable_sync() {
        let manager = SyncManager::new();
        manager.set_enabled(false);
        
        let _guard = manager.acquire_read_lock("resource1");
        
        // Should not increment counters when disabled
        assert_eq!(manager.read_count.load(Ordering::Relaxed), 0);
    }
    
    #[test]
    fn test_batch_coordinator() {
        let manager = Arc::new(SyncManager::new());
        let coordinator = BatchCoordinator::new(manager, 3);
        
        coordinator.add_operation("op1".to_string());
        coordinator.add_operation("op2".to_string());
        assert_eq!(coordinator.pending_count(), 2);
        
        coordinator.add_operation("op3".to_string());
        // Should auto-flush at batch size
        assert_eq!(coordinator.pending_count(), 0);
    }
}

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use crate::events::binlog_event::BinlogEvent;

/// Statistics for a specific event type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeStats {
    /// Number of events parsed
    pub count: u64,
    /// Total bytes processed for this event type
    pub total_bytes: u64,
    /// Total time spent parsing this event type
    pub total_parse_time: Duration,
    /// Average parse time per event
    pub avg_parse_time: Duration,
    /// Minimum parse time observed
    pub min_parse_time: Duration,
    /// Maximum parse time observed
    pub max_parse_time: Duration,
    /// Number of parse errors for this event type
    pub error_count: u64,
    /// Last time this event type was seen
    pub last_seen: SystemTime,
    /// First time this event type was seen
    pub first_seen: SystemTime,
}

impl EventTypeStats {
    pub fn new() -> Self {
        let now = SystemTime::now();
        Self {
            count: 0,
            total_bytes: 0,
            total_parse_time: Duration::ZERO,
            avg_parse_time: Duration::ZERO,
            min_parse_time: Duration::MAX,
            max_parse_time: Duration::ZERO,
            error_count: 0,
            last_seen: now,
            first_seen: now,
        }
    }

    /// Update statistics with a successful parse
    pub fn record_success(&mut self, bytes: u64, parse_time: Duration) {
        let now = SystemTime::now();
        
        if self.count == 0 {
            self.first_seen = now;
        }
        
        self.count += 1;
        self.total_bytes += bytes;
        self.total_parse_time += parse_time;
        self.last_seen = now;
        
        // Update min/max parse times
        if parse_time < self.min_parse_time {
            self.min_parse_time = parse_time;
        }
        if parse_time > self.max_parse_time {
            self.max_parse_time = parse_time;
        }
        
        // Update average
        self.avg_parse_time = self.total_parse_time / self.count as u32;
    }

    /// Record a parse error
    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.last_seen = SystemTime::now();
    }

    /// Get the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.count + self.error_count == 0 {
            100.0
        } else {
            (self.count as f64 / (self.count + self.error_count) as f64) * 100.0
        }
    }

    /// Get events per second based on time span
    pub fn events_per_second(&self) -> f64 {
        if let Ok(duration) = self.last_seen.duration_since(self.first_seen) {
            if duration.as_secs() > 0 {
                self.count as f64 / duration.as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Get bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        if let Ok(duration) = self.last_seen.duration_since(self.first_seen) {
            if duration.as_secs() > 0 {
                self.total_bytes as f64 / duration.as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
}

impl Default for EventTypeStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Overall parsing statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseStats {
    /// Statistics per event type
    pub event_type_stats: HashMap<u8, EventTypeStats>,
    /// Total events parsed across all types
    pub total_events: u64,
    /// Total bytes processed
    pub total_bytes: u64,
    /// Total parse time
    pub total_parse_time: Duration,
    /// Total errors across all event types
    pub total_errors: u64,
    /// When parsing started
    pub start_time: SystemTime,
    /// Last update time
    pub last_update: SystemTime,
    /// Current parsing position
    pub current_position: u64,
    /// Current binlog file name
    pub current_file: Option<String>,
}

impl ParseStats {
    pub fn new() -> Self {
        let now = SystemTime::now();
        Self {
            event_type_stats: HashMap::new(),
            total_events: 0,
            total_bytes: 0,
            total_parse_time: Duration::ZERO,
            total_errors: 0,
            start_time: now,
            last_update: now,
            current_position: 0,
            current_file: None,
        }
    }

    /// Get overall events per second
    pub fn overall_events_per_second(&self) -> f64 {
        if let Ok(duration) = self.last_update.duration_since(self.start_time) {
            if duration.as_secs() > 0 {
                self.total_events as f64 / duration.as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Get overall bytes per second
    pub fn overall_bytes_per_second(&self) -> f64 {
        if let Ok(duration) = self.last_update.duration_since(self.start_time) {
            if duration.as_secs() > 0 {
                self.total_bytes as f64 / duration.as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Get overall success rate
    pub fn overall_success_rate(&self) -> f64 {
        if self.total_events + self.total_errors == 0 {
            100.0
        } else {
            (self.total_events as f64 / (self.total_events + self.total_errors) as f64) * 100.0
        }
    }

    /// Get average parse time per event
    pub fn average_parse_time(&self) -> Duration {
        if self.total_events > 0 {
            self.total_parse_time / self.total_events as u32
        } else {
            Duration::ZERO
        }
    }

    /// Get the most common event types
    pub fn top_event_types(&self, limit: usize) -> Vec<(u8, &EventTypeStats)> {
        let mut types: Vec<_> = self.event_type_stats.iter()
            .map(|(&event_type, stats)| (event_type, stats))
            .collect();
        
        types.sort_by(|a, b| b.1.count.cmp(&a.1.count));
        types.truncate(limit);
        types
    }

    /// Get event type distribution as percentages
    pub fn event_type_distribution(&self) -> HashMap<u8, f64> {
        let mut distribution = HashMap::new();
        
        if self.total_events > 0 {
            for (&event_type, stats) in &self.event_type_stats {
                let percentage = (stats.count as f64 / self.total_events as f64) * 100.0;
                distribution.insert(event_type, percentage);
            }
        }
        
        distribution
    }

    /// Get parsing progress information
    pub fn get_progress_info(&self) -> ProgressInfo {
        ProgressInfo {
            current_position: self.current_position,
            current_file: self.current_file.clone(),
            events_processed: self.total_events,
            bytes_processed: self.total_bytes,
            errors_encountered: self.total_errors,
            elapsed_time: self.last_update.duration_since(self.start_time).unwrap_or(Duration::ZERO),
            events_per_second: self.overall_events_per_second(),
            bytes_per_second: self.overall_bytes_per_second(),
        }
    }
}

impl Default for ParseStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Progress information for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressInfo {
    pub current_position: u64,
    pub current_file: Option<String>,
    pub events_processed: u64,
    pub bytes_processed: u64,
    pub errors_encountered: u64,
    pub elapsed_time: Duration,
    pub events_per_second: f64,
    pub bytes_per_second: f64,
}

/// Event statistics collector
#[derive(Debug)]
pub struct EventStatsCollector {
    stats: Arc<RwLock<ParseStats>>,
    active_timers: Arc<Mutex<HashMap<u64, Instant>>>, // position -> start time
}

impl EventStatsCollector {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(RwLock::new(ParseStats::new())),
            active_timers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start timing an event parse operation
    pub fn start_event_timer(&self, position: u64) {
        if let Ok(mut timers) = self.active_timers.lock() {
            timers.insert(position, Instant::now());
        }
    }

    /// Record a successful event parse
    pub fn record_event_success(&self, event: &BinlogEvent, position: u64) {
        let parse_time = if let Ok(mut timers) = self.active_timers.lock() {
            timers.remove(&position).map(|start| start.elapsed()).unwrap_or(Duration::ZERO)
        } else {
            Duration::ZERO
        };

        if let Ok(mut stats) = self.stats.write() {
            let event_type = event.get_event_type_code();
            let event_size = event.len() as u64;

            // Update overall stats
            stats.total_events += 1;
            stats.total_bytes += event_size;
            stats.total_parse_time += parse_time;
            stats.last_update = SystemTime::now();
            stats.current_position = position;

            // Update event type specific stats
            let type_stats = stats.event_type_stats.entry(event_type).or_insert_with(EventTypeStats::new);
            type_stats.record_success(event_size, parse_time);
        }
    }

    /// Record a parse error
    pub fn record_event_error(&self, event_type: u8, position: u64) {
        // Clean up timer
        if let Ok(mut timers) = self.active_timers.lock() {
            timers.remove(&position);
        }

        if let Ok(mut stats) = self.stats.write() {
            stats.total_errors += 1;
            stats.last_update = SystemTime::now();
            stats.current_position = position;

            // Update event type specific stats
            let type_stats = stats.event_type_stats.entry(event_type).or_insert_with(EventTypeStats::new);
            type_stats.record_error();
        }
    }

    /// Update current file being processed
    pub fn set_current_file(&self, filename: String) {
        if let Ok(mut stats) = self.stats.write() {
            stats.current_file = Some(filename);
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> ParseStats {
        if let Ok(stats) = self.stats.read() {
            stats.clone()
        } else {
            ParseStats::new()
        }
    }

    /// Get statistics for a specific event type
    pub fn get_event_type_stats(&self, event_type: u8) -> Option<EventTypeStats> {
        if let Ok(stats) = self.stats.read() {
            stats.event_type_stats.get(&event_type).cloned()
        } else {
            None
        }
    }

    /// Reset all statistics
    pub fn reset(&self) {
        if let Ok(mut stats) = self.stats.write() {
            *stats = ParseStats::new();
        }
        if let Ok(mut timers) = self.active_timers.lock() {
            timers.clear();
        }
    }

    /// Get progress information
    pub fn get_progress(&self) -> ProgressInfo {
        self.get_stats().get_progress_info()
    }

    /// Export statistics to JSON
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        let stats = self.get_stats();
        serde_json::to_string_pretty(&stats)
    }

    /// Get a summary report
    pub fn get_summary_report(&self) -> String {
        let stats = self.get_stats();
        let progress = stats.get_progress_info();
        
        let mut report = String::new();
        report.push_str("=== Binlog Parser Statistics ===\n");
        report.push_str(&format!("Total Events: {}\n", stats.total_events));
        report.push_str(&format!("Total Bytes: {} ({:.2} MB)\n", 
            stats.total_bytes, 
            stats.total_bytes as f64 / 1024.0 / 1024.0));
        report.push_str(&format!("Total Errors: {}\n", stats.total_errors));
        report.push_str(&format!("Success Rate: {:.2}%\n", stats.overall_success_rate()));
        report.push_str(&format!("Events/sec: {:.2}\n", progress.events_per_second));
        report.push_str(&format!("Bytes/sec: {:.2} ({:.2} MB/s)\n", 
            progress.bytes_per_second,
            progress.bytes_per_second / 1024.0 / 1024.0));
        report.push_str(&format!("Average Parse Time: {:?}\n", stats.average_parse_time()));
        
        if let Some(ref filename) = progress.current_file {
            report.push_str(&format!("Current File: {}\n", filename));
        }
        report.push_str(&format!("Current Position: {}\n", progress.current_position));
        
        report.push_str("\n=== Top Event Types ===\n");
        for (event_type, type_stats) in stats.top_event_types(10) {
            report.push_str(&format!("Type 0x{:02x}: {} events ({:.1}%), {:.2}ms avg\n",
                event_type,
                type_stats.count,
                (type_stats.count as f64 / stats.total_events as f64) * 100.0,
                type_stats.avg_parse_time.as_secs_f64() * 1000.0
            ));
        }
        
        report
    }
}

impl Default for EventStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for EventStatsCollector {
    fn clone(&self) -> Self {
        Self {
            stats: Arc::clone(&self.stats),
            active_timers: Arc::clone(&self.active_timers),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_event_type_stats() {
        let mut stats = EventTypeStats::new();
        
        // Record some successful parses
        stats.record_success(100, Duration::from_millis(10));
        stats.record_success(200, Duration::from_millis(20));
        stats.record_success(150, Duration::from_millis(15));
        
        assert_eq!(stats.count, 3);
        assert_eq!(stats.total_bytes, 450);
        assert_eq!(stats.min_parse_time, Duration::from_millis(10));
        assert_eq!(stats.max_parse_time, Duration::from_millis(20));
        
        // Record an error
        stats.record_error();
        assert_eq!(stats.error_count, 1);
        assert_eq!(stats.success_rate(), 75.0); // 3 success out of 4 total
    }

    #[test]
    fn test_stats_collector() {
        let collector = EventStatsCollector::new();
        
        // Simulate parsing some events
        collector.start_event_timer(100);
        thread::sleep(Duration::from_millis(1));
        
        // Create a mock event for testing
        let mock_event = BinlogEvent::IgnorableLogEvent;
        collector.record_event_success(&mock_event, 100);
        
        let stats = collector.get_stats();
        assert_eq!(stats.total_events, 1);
        assert!(stats.total_parse_time > Duration::ZERO);
    }

    #[test]
    fn test_progress_info() {
        let collector = EventStatsCollector::new();
        collector.set_current_file("test.binlog".to_string());
        
        let progress = collector.get_progress();
        assert_eq!(progress.current_file, Some("test.binlog".to_string()));
        assert_eq!(progress.events_processed, 0);
    }
}
use std::sync::Arc;
use async_trait::async_trait;
use common::err::decode_error::ReError;
use crate::row::row_data::{RowData, UpdateRowData};
use crate::events::protocol::table_map_event::TableMapEvent;

/// Trait for handling row-level events in binlog parsing
pub trait RowEventHandler: Send + Sync {
    /// Called when a row is inserted
    fn on_row_insert(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError>;
    
    /// Called when a row is updated
    fn on_row_update(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<(), ReError>;
    
    /// Called when a row is deleted
    fn on_row_delete(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError>;
    
    /// Called when processing starts for a table
    fn on_table_start(&self, table: &TableMapEvent) -> Result<(), ReError> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Called when processing ends for a table
    fn on_table_end(&self, table: &TableMapEvent) -> Result<(), ReError> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Called when an error occurs during row processing
    fn on_error(&self, table: &TableMapEvent, error: &ReError) -> Result<(), ReError> {
        // Default implementation propagates the error
        Err(ReError::String(format!("Row processing error: {:?}", error)))
    }
}

/// Async version of RowEventHandler for non-blocking operations
#[async_trait]
pub trait AsyncRowEventHandler: Send + Sync {
    /// Called when a row is inserted (async)
    async fn on_row_insert_async(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError>;
    
    /// Called when a row is updated (async)
    async fn on_row_update_async(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<(), ReError>;
    
    /// Called when a row is deleted (async)
    async fn on_row_delete_async(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError>;
    
    /// Called when processing starts for a table (async)
    async fn on_table_start_async(&self, table: &TableMapEvent) -> Result<(), ReError> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Called when processing ends for a table (async)
    async fn on_table_end_async(&self, table: &TableMapEvent) -> Result<(), ReError> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Called when an error occurs during row processing (async)
    async fn on_error_async(&self, table: &TableMapEvent, error: &ReError) -> Result<(), ReError> {
        // Default implementation propagates the error
        Err(ReError::String(format!("Async row processing error: {:?}", error)))
    }
}

/// Registry for managing row event handlers
pub struct RowEventHandlerRegistry {
    sync_handlers: Vec<Arc<dyn RowEventHandler>>,
    async_handlers: Vec<Arc<dyn AsyncRowEventHandler>>,
}

impl std::fmt::Debug for RowEventHandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowEventHandlerRegistry")
            .field("sync_handlers_count", &self.sync_handlers.len())
            .field("async_handlers_count", &self.async_handlers.len())
            .finish()
    }
}

impl RowEventHandlerRegistry {
    pub fn new() -> Self {
        Self {
            sync_handlers: Vec::new(),
            async_handlers: Vec::new(),
        }
    }
    
    /// Register a synchronous row event handler
    pub fn register_sync_handler(&mut self, handler: Arc<dyn RowEventHandler>) {
        self.sync_handlers.push(handler);
    }
    
    /// Register an asynchronous row event handler
    pub fn register_async_handler(&mut self, handler: Arc<dyn AsyncRowEventHandler>) {
        self.async_handlers.push(handler);
    }
    
    /// Remove all handlers
    pub fn clear_handlers(&mut self) {
        self.sync_handlers.clear();
        self.async_handlers.clear();
    }
    
    /// Get count of registered sync handlers
    pub fn sync_handler_count(&self) -> usize {
        self.sync_handlers.len()
    }
    
    /// Get count of registered async handlers
    pub fn async_handler_count(&self) -> usize {
        self.async_handlers.len()
    }
    
    /// Process row insert event with all registered handlers
    pub fn process_insert(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError> {
        for handler in &self.sync_handlers {
            handler.on_row_insert(table, row)?;
        }
        Ok(())
    }
    
    /// Process row update event with all registered handlers
    pub fn process_update(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<(), ReError> {
        for handler in &self.sync_handlers {
            handler.on_row_update(table, before, after)?;
        }
        Ok(())
    }
    
    /// Process row delete event with all registered handlers
    pub fn process_delete(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError> {
        for handler in &self.sync_handlers {
            handler.on_row_delete(table, row)?;
        }
        Ok(())
    }
    
    /// Process error event with all registered handlers
    pub fn on_error(&self, table: &TableMapEvent, error: &ReError) -> Result<(), ReError> {
        for handler in &self.sync_handlers {
            handler.on_error(table, error)?;
        }
        Ok(())
    }
    
    /// Process table start event with all registered handlers
    pub fn process_table_start(&self, table: &TableMapEvent) -> Result<(), ReError> {
        for handler in &self.sync_handlers {
            handler.on_table_start(table)?;
        }
        Ok(())
    }
    
    /// Process table end event with all registered handlers
    pub fn process_table_end(&self, table: &TableMapEvent) -> Result<(), ReError> {
        for handler in &self.sync_handlers {
            handler.on_table_end(table)?;
        }
        Ok(())
    }
    
    /// Process async row insert event with all registered async handlers
    pub async fn process_insert_async(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError> {
        for handler in &self.async_handlers {
            handler.on_row_insert_async(table, row).await?;
        }
        Ok(())
    }
    
    /// Process async row update event with all registered async handlers
    pub async fn process_update_async(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<(), ReError> {
        for handler in &self.async_handlers {
            handler.on_row_update_async(table, before, after).await?;
        }
        Ok(())
    }
    
    /// Process async row delete event with all registered async handlers
    pub async fn process_delete_async(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError> {
        for handler in &self.async_handlers {
            handler.on_row_delete_async(table, row).await?;
        }
        Ok(())
    }
    
    /// Process async table start event with all registered async handlers
    pub async fn process_table_start_async(&self, table: &TableMapEvent) -> Result<(), ReError> {
        for handler in &self.async_handlers {
            handler.on_table_start_async(table).await?;
        }
        Ok(())
    }
    
    /// Process async table end event with all registered async handlers
    pub async fn process_table_end_async(&self, table: &TableMapEvent) -> Result<(), ReError> {
        for handler in &self.async_handlers {
            handler.on_table_end_async(table).await?;
        }
        Ok(())
    }
}

impl Default for RowEventHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// A simple logging row event handler for debugging and monitoring
#[derive(Debug)]
pub struct LoggingRowEventHandler {
    table_filter: Option<String>,
}

impl LoggingRowEventHandler {
    pub fn new() -> Self {
        Self {
            table_filter: None,
        }
    }
    
    pub fn with_table_filter(table_name: String) -> Self {
        Self {
            table_filter: Some(table_name),
        }
    }
    
    fn should_process(&self, table: &TableMapEvent) -> bool {
        match &self.table_filter {
            Some(filter) => table.get_table_name() == *filter,
            None => true,
        }
    }
}

impl RowEventHandler for LoggingRowEventHandler {
    fn on_row_insert(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError> {
        if self.should_process(table) {
            tracing::info!(
                "INSERT into {}.{}: {} columns",
                table.get_database_name(),
                table.get_table_name(),
                row.cells.len()
            );
        }
        Ok(())
    }
    
    fn on_row_update(&self, table: &TableMapEvent, before: &RowData, after: &RowData) -> Result<(), ReError> {
        if self.should_process(table) {
            tracing::info!(
                "UPDATE {}.{}: {} columns",
                table.get_database_name(),
                table.get_table_name(),
                before.cells.len()
            );
        }
        Ok(())
    }
    
    fn on_row_delete(&self, table: &TableMapEvent, row: &RowData) -> Result<(), ReError> {
        if self.should_process(table) {
            tracing::info!(
                "DELETE from {}.{}: {} columns",
                table.get_database_name(),
                table.get_table_name(),
                row.cells.len()
            );
        }
        Ok(())
    }
    
    fn on_table_start(&self, table: &TableMapEvent) -> Result<(), ReError> {
        if self.should_process(table) {
            tracing::debug!(
                "Starting processing table {}.{}",
                table.get_database_name(),
                table.get_table_name()
            );
        }
        Ok(())
    }
    
    fn on_table_end(&self, table: &TableMapEvent) -> Result<(), ReError> {
        if self.should_process(table) {
            tracing::debug!(
                "Finished processing table {}.{}",
                table.get_database_name(),
                table.get_table_name()
            );
        }
        Ok(())
    }
}

impl Default for LoggingRowEventHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// A counting row event handler for statistics
#[derive(Debug, Default)]
pub struct CountingRowEventHandler {
    insert_count: std::sync::atomic::AtomicU64,
    update_count: std::sync::atomic::AtomicU64,
    delete_count: std::sync::atomic::AtomicU64,
}

impl CountingRowEventHandler {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn get_insert_count(&self) -> u64 {
        self.insert_count.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    pub fn get_update_count(&self) -> u64 {
        self.update_count.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    pub fn get_delete_count(&self) -> u64 {
        self.delete_count.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    pub fn get_total_count(&self) -> u64 {
        self.get_insert_count() + self.get_update_count() + self.get_delete_count()
    }
    
    pub fn reset_counts(&self) {
        self.insert_count.store(0, std::sync::atomic::Ordering::Relaxed);
        self.update_count.store(0, std::sync::atomic::Ordering::Relaxed);
        self.delete_count.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

impl RowEventHandler for CountingRowEventHandler {
    fn on_row_insert(&self, _table: &TableMapEvent, _row: &RowData) -> Result<(), ReError> {
        self.insert_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    
    fn on_row_update(&self, _table: &TableMapEvent, _before: &RowData, _after: &RowData) -> Result<(), ReError> {
        self.update_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    
    fn on_row_delete(&self, _table: &TableMapEvent, _row: &RowData) -> Result<(), ReError> {
        self.delete_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::events::protocol::table_map_event::TableMapEvent;
    use crate::row::row_data::RowData;
    use common::binlog::column::column_value::SrcColumnValue;

    #[test]
    fn test_row_event_handler_registry() {
        let mut registry = RowEventHandlerRegistry::new();
        
        // Test empty registry
        assert_eq!(registry.sync_handler_count(), 0);
        assert_eq!(registry.async_handler_count(), 0);
        
        // Register handlers
        let logging_handler = Arc::new(LoggingRowEventHandler::new());
        let counting_handler = Arc::new(CountingRowEventHandler::new());
        
        registry.register_sync_handler(logging_handler);
        registry.register_sync_handler(counting_handler.clone());
        
        assert_eq!(registry.sync_handler_count(), 2);
        
        // Test processing
        let table = TableMapEvent::default();
        let row = RowData::new_with_cells(vec![Some(SrcColumnValue::Int(1))]);
        
        registry.process_insert(&table, &row).unwrap();
        assert_eq!(counting_handler.get_insert_count(), 1);
        
        registry.process_update(&table, &row, &row).unwrap();
        assert_eq!(counting_handler.get_update_count(), 1);
        
        registry.process_delete(&table, &row).unwrap();
        assert_eq!(counting_handler.get_delete_count(), 1);
        
        assert_eq!(counting_handler.get_total_count(), 3);
        
        // Test clear
        registry.clear_handlers();
        assert_eq!(registry.sync_handler_count(), 0);
    }

    #[test]
    fn test_counting_handler() {
        let handler = CountingRowEventHandler::new();
        let table = TableMapEvent::default();
        let row = RowData::new_with_cells(vec![Some(SrcColumnValue::Int(1))]);
        
        // Test initial counts
        assert_eq!(handler.get_insert_count(), 0);
        assert_eq!(handler.get_update_count(), 0);
        assert_eq!(handler.get_delete_count(), 0);
        assert_eq!(handler.get_total_count(), 0);
        
        // Test counting
        handler.on_row_insert(&table, &row).unwrap();
        handler.on_row_insert(&table, &row).unwrap();
        assert_eq!(handler.get_insert_count(), 2);
        
        handler.on_row_update(&table, &row, &row).unwrap();
        assert_eq!(handler.get_update_count(), 1);
        
        handler.on_row_delete(&table, &row).unwrap();
        assert_eq!(handler.get_delete_count(), 1);
        
        assert_eq!(handler.get_total_count(), 4);
        
        // Test reset
        handler.reset_counts();
        assert_eq!(handler.get_total_count(), 0);
    }

    #[test]
    fn test_logging_handler_with_filter() {
        let handler = LoggingRowEventHandler::with_table_filter("test_table".to_string());
        let mut table = TableMapEvent::default();
        table.set_table_name("test_table".to_string());
        let row = RowData::new_with_cells(vec![Some(SrcColumnValue::Int(1))]);
        
        // Should not panic and should process the table
        handler.on_row_insert(&table, &row).unwrap();
        handler.on_table_start(&table).unwrap();
        handler.on_table_end(&table).unwrap();
        
        // Test with different table name
        table.set_table_name("other_table".to_string());
        handler.on_row_insert(&table, &row).unwrap(); // Should still work but not log
    }
}
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::Serialize;
use crate::events::log_position::LogPosition;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

#[derive(Debug, Serialize, Clone)]
pub struct LogContext {
    pub format_description: Arc<FormatDescriptionEvent>,

    pub log_position: Arc<RwLock<LogPosition>>,

    pub compatiable_percona: bool,

    map_of_table: Arc<RwLock<HashMap<u64, TableMapEvent>>>,

    // /// save current gtid log event
    // pub gtid_log_event: Box<dyn LogEvent>,
}

impl Default for LogContext {
    fn default() -> Self {
        LogContext {
            format_description: Arc::new(FormatDescriptionEvent::default()),
            log_position: Arc::new(RwLock::new(LogPosition::default())),
            compatiable_percona: false,
            map_of_table: Arc::new(RwLock::new(HashMap::<u64, TableMapEvent>::new())),
            // gtid_log_event: Box::default(),
        }
    }
}

impl LogContext {
    pub fn new(log_position: LogPosition) -> Self {
        LogContext::new_with_format_description(log_position, FormatDescriptionEvent::default())
    }

    pub fn new_with_format_description(log_position: LogPosition, format_description: FormatDescriptionEvent) -> Self {
        LogContext {
            format_description: Arc::new(format_description),
            log_position: Arc::new(RwLock::new(log_position)),
            compatiable_percona: false,
            map_of_table: Arc::new(RwLock::new(HashMap::<u64, TableMapEvent>::new())),
            // gtid_log_event: Box::default(),
        }
    }

    pub fn set_format_description(&mut self, fd: FormatDescriptionEvent) {
        self.format_description = Arc::new(fd);
    }

    pub fn get_format_description(&self) -> Arc<FormatDescriptionEvent> {
        self.format_description.clone()
    }

    pub fn set_log_position(&mut self, lp: LogPosition) {
        self.log_position = Arc::new(RwLock::new(lp));
    }

    pub fn get_log_position(&self) -> Arc<RwLock<LogPosition>> {
        self.log_position.clone()
    }

    pub fn set_log_position_with_offset(&mut self, pos: u32) {
        self.log_position.write().unwrap().set_position(pos as u64);
    }

    pub fn set_compatiable_percona(&mut self, compatiable_percona: bool) {
        self.compatiable_percona = compatiable_percona;
    }

    pub fn is_compatiable_percona(&self) -> bool{
        self.compatiable_percona
    }

    pub fn get_map_of_table(&self, table_id: &u64) -> Option<TableMapEvent> {
        let binding = self.map_of_table.read().unwrap();
        let if_exist = binding.get(table_id);

        return if if_exist.is_some() {
            Some(if_exist.unwrap().clone())
        } else {
            None
        }
    }

    pub fn put_table(&mut self, table_id: u64, table_map_event: TableMapEvent) {
        self.map_of_table.write().unwrap().insert(table_id, table_map_event);
    }

    // pub fn set_gtid_log_event(&mut self, event: Box<dyn LogEvent>) {
    //     self.gtid_log_event = event;
    // }
}

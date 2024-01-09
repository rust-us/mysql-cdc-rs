use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, RwLock};
use nom::ExtendInto;
use serde::Serialize;
use crate::events::log_position::LogPosition;
use crate::events::log_stat::LogStat;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

pub trait ILogContext {
    fn new(log_position: LogPosition) -> LogContext;

    fn new_with_format_description(log_position: LogPosition, log_stat: LogStat,
                                   format_description: FormatDescriptionEvent) -> LogContext;

    fn set_format_description(&mut self, fd: FormatDescriptionEvent);
    fn get_format_description(&self) -> Arc<FormatDescriptionEvent>;

    fn set_log_position(&mut self, log_pos: LogPosition);
    fn get_log_position(&self) -> Arc<RwLock<LogPosition>>;
    fn set_log_position_with_offset(&mut self, pos: u32);

    fn log_stat_add(&mut self);
    fn log_stat_process_count(&self) -> u64;

    fn set_compatiable_percona(&mut self, compatiable_percona: bool);
    fn is_compatiable_percona(&self) -> bool;

    /// get TableMapEvent maps with table_id, Clone Data
    fn get_map_of_table(&self, table_id: &u64) -> Option<TableMapEvent>;
    fn put_table(&mut self, table_id: u64, table_map_event: TableMapEvent);

    /// 输出格式化
    fn stat_fmt(&mut self) -> String;
}

#[derive(Debug, Serialize, Clone)]
pub struct LogContext {
    /// The FormatDescription declaration of the current binlog
    format_description: Arc<FormatDescriptionEvent>,

    /// binlog position
    log_position: Arc<RwLock<LogPosition>>,

    /// binlog monitor and stat
    log_stat: Arc<RwLock<LogStat>>,

    /// is compatiable , default value is false
    compatiable_percona: bool,

    /// TableMapEvent maps with table_id
    map_of_table: Arc<RwLock<HashMap<u64, TableMapEvent>>>,

    // /// save current gtid log event
    // pub gtid_log_event: Box<dyn LogEvent>,
}

impl Default for LogContext {
    fn default() -> Self {
        LogContext {
            format_description: Arc::new(FormatDescriptionEvent::default()),
            log_position: Arc::new(RwLock::new(LogPosition::default())),
            log_stat: Arc::new(RwLock::new(LogStat::default())),
            compatiable_percona: false,
            map_of_table: Arc::new(RwLock::new(HashMap::<u64, TableMapEvent>::new())),
            // gtid_log_event: Box::default(),
        }
    }
}

impl ILogContext for LogContext {
    fn new(log_position: LogPosition) -> LogContext {
        LogContext::new_with_format_description(log_position, LogStat::default(), FormatDescriptionEvent::default())
    }

    fn new_with_format_description(log_position: LogPosition, log_stat: LogStat,
                                       format_description: FormatDescriptionEvent) -> LogContext {
        LogContext {
            format_description: Arc::new(format_description),
            log_position: Arc::new(RwLock::new(log_position)),
            log_stat: Arc::new(RwLock::new(log_stat)),
            compatiable_percona: false,
            map_of_table: Arc::new(RwLock::new(HashMap::<u64, TableMapEvent>::new())),
            // gtid_log_event: Box::default(),
        }
    }

    fn set_format_description(&mut self, fd: FormatDescriptionEvent) {
        self.format_description = Arc::new(fd);
    }

    fn get_format_description(&self) -> Arc<FormatDescriptionEvent> {
        self.format_description.clone()
    }

    fn set_log_position(&mut self, log_pos: LogPosition) {
        self.log_position = Arc::new(RwLock::new(log_pos));
    }

    fn get_log_position(&self) -> Arc<RwLock<LogPosition>> {
        self.log_position.clone()
    }

    fn set_log_position_with_offset(&mut self, pos: u32) {
        self.log_position.write().unwrap().set_position(pos as u64);
    }

    fn log_stat_add(&mut self) {
        self.log_stat.write().unwrap().add();
    }

    fn log_stat_process_count(&self) -> u64 {
        self.log_stat.read().unwrap().clone().get_process_count()
    }

    fn set_compatiable_percona(&mut self, compatiable_percona: bool) {
        self.compatiable_percona = compatiable_percona;
    }

    fn is_compatiable_percona(&self) -> bool{
        self.compatiable_percona
    }

    fn get_map_of_table(&self, table_id: &u64) -> Option<TableMapEvent> {
        let binding = self.map_of_table.read().unwrap();
        let if_exist = binding.get(table_id);

        return if if_exist.is_some() {
            Some(if_exist.unwrap().clone())
        } else {
            None
        }
    }

    fn put_table(&mut self, table_id: u64, table_map_event: TableMapEvent) {
        self.map_of_table.write().unwrap().insert(table_id, table_map_event);
    }

    fn stat_fmt(&mut self) -> String {
        let pos = self.log_position.read().unwrap();
        let stat = self.log_stat.read().unwrap();

        format!("Current server_version:{}, Current binlog_version:{}, Current Binlog File: {}, Current position: {}, Total process_count:{}",
                &*self.format_description.server_version, &*self.format_description.binlog_version.to_string(),
                &*pos.get_file_name(), &*pos.get_position().to_string(),
                &*stat.get_process_count().to_string())
    }

    // pub fn set_gtid_log_event(&mut self, event: Box<dyn LogEvent>) {
    //     self.gtid_log_event = event;
    // }
}


#[cfg(test)]
mod test {
    use crate::events::log_context::{ILogContext, LogContext};
    use crate::events::log_position::LogPosition;

    #[test]
    fn test() {
        let mut _context:LogContext = LogContext::new(LogPosition::new("BytesBinlogReader"));
        _context.set_log_position_with_offset(66);

        assert_eq!("Current server_version:5.0, Current binlog_version:4, Current Binlog File: BytesBinlogReader, Current position: 66, Total process_count:0",
                   _context.stat_fmt());
    }
}
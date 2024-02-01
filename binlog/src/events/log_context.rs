use std::cell::RefCell;
use std::ops::Add;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use nom::ExtendInto;
use serde::Serialize;
use crate::alias::mysql::events::gtid_log_event::GtidLogEvent;
use crate::alias::mysql::gtid::gtid_set::GtidSet;
use crate::events::declare::log_event::LogEvent;
use crate::events::log_position::{LogPosition, LogPositionRef};
use crate::events::log_stat::{LogStat, LogStatRef};
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::events::protocol::table_map_event::TableMapEvent;

pub trait ILogContext {
    fn new(log_position: LogPosition) -> LogContext;

    fn new_with_gtid(log_position: LogPosition, gtid_set: GtidSet) -> LogContext;

    fn new_with_format_description(log_position: LogPosition, log_stat: LogStat,
                                   format_description: FormatDescriptionEvent, gtid_set: Option<GtidSet>) -> LogContext;

    fn get_format_description(&self) -> Arc<FormatDescriptionEvent>;
    fn set_format_description(&mut self, fd: FormatDescriptionEvent);

    fn get_log_position(&self) -> LogPositionRef;
    fn set_log_position(&mut self, log_pos: LogPosition);
    fn update_log_position_with_offset(&mut self, pos: u64);

    fn get_log_stat(&self) -> LogStatRef;
    fn update_log_stat_add(&mut self);

    /// 当前已经处理的binlog数量
    fn get_log_stat_process_count(&self) -> u64;

    fn is_compatiable_percona(&self) -> bool;
    fn set_compatiable_percona(&mut self, compatiable_percona: bool);

    fn get_table_len(&self) -> usize;
    /// get TableMapEvent maps with table_id, Clone Data
    fn get_table(&self, table_id: &u64) -> Option<Ref<u64, TableMapEvent>>;
    fn put_table(&mut self, table_id: u64, table_map_event: TableMapEvent);
    /// 清空 map_of_table 集合
    fn clear_all_table(&mut self);

    fn get_gtid_set_as_mut(&mut self) -> Option<&mut GtidSet>;
    fn get_gtid_set(&self) -> Option<&GtidSet>;

    /// gtid_set 数据更新
    fn update_gtid_set(&mut self, gtid_var: String);

    fn get_gtid_log_event(&self) -> Option<&GtidLogEvent>;
    fn set_gtid_log_event(&mut self, gtid_log_event: GtidLogEvent);

    /// 输出格式化
    fn stat_fmt(&mut self) -> String;
}

pub type LogContextRef = Rc<RefCell<LogContext>>;

#[derive(Debug, Clone)]
pub struct LogContext {
    /// The FormatDescription declaration of the current binlog
    format_description: Arc<FormatDescriptionEvent>,

    /// binlog position
    log_position: LogPositionRef,

    /// binlog monitor and stat
    log_stat: LogStatRef,

    /// is compatiable , default value is false
    compatiable_percona: bool,

    /// TableMapEvent maps with table_id
    map_of_table: Arc<DashMap<u64, TableMapEvent>>,

    gtid_set: Option<GtidSet>,

    /// save current gtid log event
    gtid_log_event: Option<GtidLogEvent>,
}

impl Default for LogContext {
    fn default() -> Self {
        LogContext {
            format_description: Arc::new(FormatDescriptionEvent::default()),
            log_position: Arc::new(RwLock::new(LogPosition::default())),
            log_stat: Arc::new(RwLock::new(LogStat::default())),
            compatiable_percona: false,
            map_of_table: Arc::new(DashMap::<u64, TableMapEvent>::new()),
            gtid_set: None,
            gtid_log_event: None,
        }
    }
}

impl ILogContext for LogContext {
    fn new(log_position: LogPosition) -> LogContext {
        LogContext::new_with_format_description(log_position, LogStat::default(), FormatDescriptionEvent::default(), None)
    }

    fn new_with_gtid(log_position: LogPosition, gtid_set: GtidSet) -> LogContext {
        LogContext::new_with_format_description(log_position, LogStat::default(), FormatDescriptionEvent::default(), Some(gtid_set))
    }

    fn new_with_format_description(log_position: LogPosition, log_stat: LogStat,
                                   format_description: FormatDescriptionEvent, gtid_set: Option<GtidSet>) -> LogContext {
        LogContext {
            format_description: Arc::new(format_description),
            log_position: Arc::new(RwLock::new(log_position)),
            log_stat: Arc::new(RwLock::new(log_stat)),
            compatiable_percona: false,
            map_of_table: Arc::new(DashMap::<u64, TableMapEvent>::new()),
            gtid_set,
            gtid_log_event: None,
        }
    }

    fn get_format_description(&self) -> Arc<FormatDescriptionEvent> {
        self.format_description.clone()
    }

    fn set_format_description(&mut self, fd: FormatDescriptionEvent) {
        self.format_description = Arc::new(fd);
    }

    fn get_log_position(&self) -> LogPositionRef {
        self.log_position.clone()
    }

    fn set_log_position(&mut self, log_pos: LogPosition) {
        self.log_position = Arc::new(RwLock::new(log_pos));
    }

    fn update_log_position_with_offset(&mut self, pos: u64) {
        self.log_position.write().unwrap().set_position(pos);
    }

    fn get_log_stat(&self) -> LogStatRef {
        self.log_stat.clone()
    }

    fn update_log_stat_add(&mut self) {
        self.log_stat.write().unwrap().add();
    }

    fn get_log_stat_process_count(&self) -> u64 {
        self.log_stat.read().unwrap().clone().get_process_count()
    }

    fn is_compatiable_percona(&self) -> bool{
        self.compatiable_percona
    }

    fn set_compatiable_percona(&mut self, compatiable_percona: bool) {
        self.compatiable_percona = compatiable_percona;
    }

    fn get_table_len(&self) -> usize {
        if self.map_of_table.is_empty() {
            return 0;
        }

        self.map_of_table.len()
    }

    fn get_table(&self, table_id: &u64) -> Option<Ref<u64, TableMapEvent>> {
        if self.map_of_table.contains_key(table_id) {
            let ref_: Ref<u64, TableMapEvent>  = self.map_of_table.get(table_id).unwrap();

            return Some(ref_);
        }

        None
    }

    fn put_table(&mut self, table_id: u64, table_map_event: TableMapEvent) {
        self.map_of_table.insert(table_id, table_map_event);
    }

    fn clear_all_table(&mut self) {
        if self.map_of_table.is_empty() {
            return;
        }

        self.map_of_table.clear();
    }

    fn get_gtid_set_as_mut(&mut self) -> Option<&mut GtidSet> {
        if self.gtid_set.is_none() {
            return None;
        }

        self.gtid_set.as_mut()
    }

    fn get_gtid_set(&self) -> Option<&GtidSet> {
        if self.gtid_set.is_none() {
            return None;
        }

        self.gtid_set.as_ref()
    }

    fn update_gtid_set(&mut self, gtid_var: String) {
        if self.gtid_set.is_some() {
            let mut _tmp = self.gtid_set.clone().unwrap();
            _tmp.add_gtid_str(gtid_var);
            self.gtid_set = Some(_tmp.clone());
        }
    }

    fn get_gtid_log_event(&self) -> Option<&GtidLogEvent> {
        if self.gtid_log_event.is_none() {
            return None;
        }

        self.gtid_log_event.as_ref()
    }

    fn set_gtid_log_event(&mut self, gtid_log_event: GtidLogEvent) {
        self.gtid_log_event = Some(gtid_log_event);
    }

    fn stat_fmt(&mut self) -> String {
        let pos = self.log_position.read().unwrap();
        let stat = self.log_stat.read().unwrap();

        format!("Current server_version:{}, Current binlog_version:{}, Current Binlog File: {}, Current position: {}, Total process_count:{}",
                &*self.format_description.server_version, &*self.format_description.binlog_version.to_string(),
                &*pos.get_file_name(), &*pos.get_position().to_string(),
                &*stat.get_process_count().to_string())
    }
}

#[cfg(test)]
mod test {
    use dashmap::mapref::one::Ref;
    use crate::alias::mysql::gtid::gtid_set::GtidSet;
    use crate::events::log_context::{ILogContext, LogContext};
    use crate::events::log_position::LogPosition;
    use crate::events::protocol::table_map_event::TableMapEvent;

    #[test]
    fn test() {
        let mut _context:LogContext = LogContext::new(LogPosition::new("BytesBinlogReader"));
        _context.update_log_position_with_offset(66);

        assert_eq!("Current server_version:5.0, Current binlog_version:4, Current Binlog File: BytesBinlogReader, Current position: 66, Total process_count:0",
                   _context.stat_fmt());
    }

    #[test]
    fn test_update_gtid_set() {
        let mut context:LogContext = LogContext::new_with_gtid(
            LogPosition::new("BytesBinlogReader"),
            GtidSet::parse(String::from("726757ad-4455-11e8-ae04-0242ac110002:1-3:4")).unwrap());
        context.update_log_position_with_offset(66);

        assert!(context.gtid_set.is_some());

        context.update_gtid_set("726757ad-4455-11e8-ae04-0242ac110066:1-3".to_string());

        let set = context.gtid_set.unwrap();
        assert!(set.contains_key("726757ad-4455-11e8-ae04-0242ac110066"));
        assert!(!set.contains_key("aaa"));
    }

    #[test]
    fn test_xxx_map_of_table() {
        let mut context:LogContext = LogContext::new_with_gtid(
            LogPosition::new("BytesBinlogReader"),
            GtidSet::parse(String::from("726757ad-4455-11e8-ae04-0242ac110002:1-3:4")).unwrap());
        context.update_log_position_with_offset(66);

        let mut e = TableMapEvent::default();
        e.table_id = 1;
        e.flags = 1;
        e.set_table_name(String::from("t1"));

        context.put_table(1, e);
        assert_eq!(context.get_table_len(), 1);

        {
            let item: Option<Ref<u64, TableMapEvent>> = context.get_table(&1);
            assert!(item.is_some());
            assert_eq!(item.unwrap().get_table_name().as_str(), "t1");
        }

        {
            let item2: Option<Ref<u64, TableMapEvent>> = context.get_table(&2);
            assert!(item2.is_none());
        }

        context.clear_all_table();
        assert_eq!(context.get_table_len(), 0);
    }
}
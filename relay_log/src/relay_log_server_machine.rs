use std::collections::HashMap;
use std::sync::RwLock;

use chrono::Local;
use lazy_static::lazy_static;
use tracing::{info, warn};

use binlog::events::binlog_event::BinlogEvent;
use common::err::CResult;
use common::schema::rc_task::RcTask;
use crate::relay_log::RelayLog;

lazy_static! {
    static ref INS: RelayLogServerMachine = RelayLogServerMachine {
        t: Local::now().timestamp_millis(),
        rc_task: RwLock::new(HashMap::<String, RcTask>::new()),
    };
}

#[derive(Debug)]
pub struct RelayLogServerMachine {
    t: i64,
    // task信息（线程安全）
    rc_task: RwLock<HashMap<String, RcTask>>,
}

unsafe impl Sync for RelayLogServerMachine {}

impl RelayLogServerMachine {
    /// 单例
    pub fn get_instance() -> &'static RelayLogServerMachine {
        &INS
    }

    /// 处理binlog事件
    pub fn process_binlog_event(event: &BinlogEvent) -> CResult<()> {
        let relay_entity = RelayLog::from_binlog_event(event);

        let a = Self::get_instance();

        info!("relay_entity: {:?}", relay_entity);

        let db_name = relay_entity.get_database_name();
        let table_name = relay_entity.get_table_name();

        // todo..
        Ok(())
    }

    pub fn add_task(&self, task: RcTask) -> CResult<bool> {
        let mut tasks = self.rc_task.write().unwrap();
        if tasks.contains_key(&task.task_id) {
            tasks.remove(&task.task_id);
            warn!("更新task..");
        }
        tasks.insert(task.task_id.clone(), task);
        Ok(true)
    }
}
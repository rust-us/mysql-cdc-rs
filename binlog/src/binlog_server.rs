use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use common::binlog::column::column_type::SrcColumnType;
use common::err::decode_error::ReError;
use common::server::Server;
use crate::ast::query_parser::TableInfoBuilder;
use crate::events::protocol::table_map_event::TableMapEvent;

lazy_static! {
    /// 临时验证，作废
    pub static ref TABLE_MAP: Arc<Mutex<HashMap<u64, Vec<SrcColumnType >>>> =
        Arc::new(Mutex::new(HashMap::new()));

    /// 临时验证，作废
    pub static ref TABLE_MAP_META: Arc<Mutex<HashMap<u64, Vec<u16 >>>> =
        Arc::new(Mutex::new(HashMap::new()));

    /// 临时验证，作废
    pub static ref TABLE_MAP_EVENT: Arc<Mutex<HashMap<u64, TableMapEvent>>> =
        Arc::new(Mutex::new(HashMap::new()));

    /// 维护全局唯一的表ID 与 TableInfo 的映射关系
    static ref TABLE_INFO_MAPS: Arc<Mutex<HashMap<u64, Option<TableInfoBuilder >>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

#[derive(Debug)]
pub struct BinlogServer {

}

unsafe impl Send for BinlogServer {}

#[async_trait::async_trait]
impl Server for BinlogServer {
    async fn start(&mut self) -> Result<(), ReError> {
        println!("BinlogServer start");

        Ok(())
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        println!("BinlogServer shutdown");

        Ok(())
    }
}

impl BinlogServer {
    pub fn new() -> Self {
        BinlogServer {

        }
    }

    // pub fn put_table_info(&mut self, table_info: Option<TableInfo>) {
    //     TABLE_INFO_MAPS.lock().unwrap().insert()
    // }
}

#[cfg(test)]
mod test {
    use tracing::debug;
    use common::log::tracing_factory::TracingFactory;

    #[test]
    fn test() {
        TracingFactory::init_log(true);

        debug!("test");
    }
}
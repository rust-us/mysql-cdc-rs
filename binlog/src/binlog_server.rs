use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use common::lifecycle::lifecycle::Lifecycle;
use crate::column::column_type::ColumnType;
use crate::events::protocol::table_map_event::TableMapEvent;

lazy_static! {
    pub static ref TABLE_MAP: Arc<Mutex<HashMap<u64, Vec<ColumnType >>>> =
        Arc::new(Mutex::new(HashMap::new()));

    pub static ref TABLE_MAP_META: Arc<Mutex<HashMap<u64, Vec<u16 >>>> =
        Arc::new(Mutex::new(HashMap::new()));

    pub static ref TABLE_MAP_EVENT: Arc<Mutex<HashMap<u64, TableMapEvent>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

pub struct BinlogServer {

}

impl Lifecycle for BinlogServer {
    fn setup() {
        //.
    }

    fn start() {
        //.
    }

    fn stop() {
        //.
    }

    fn pause() {
        //.
    }
}

impl BinlogServer {

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
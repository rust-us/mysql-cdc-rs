use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::web_error::WResult;
use crate::wss::strategy::WSSStrategy;

pub struct UnknownStrategyEvent {
    _inner_data: HashMap<String, String>
}

impl WSSStrategy for UnknownStrategyEvent {
    fn action(&mut self, rt: Arc<Runtime>) -> WResult<Option<String>> {
        Ok(Some("Unknown".to_string()))
    }

    fn code(&self) -> i16 {
        -1
    }
}

impl UnknownStrategyEvent {
    pub fn new(_inner_data: HashMap<String, String>) -> Self {
        UnknownStrategyEvent {
            _inner_data
        }
    }
}
use std::collections::HashMap;
use crate::web_error::WResult;
use crate::wss::strategy::WSSStrategy;

pub struct UnknowStrategyEvent {
    _inner_data: HashMap<String, String>
}

impl WSSStrategy for UnknowStrategyEvent {
    fn action(&self) -> WResult<Option<String>> {
        Ok(Some("UnknowStrategy".to_string()))
    }

    fn code(&self) -> i16 {
        -1
    }
}

impl UnknowStrategyEvent {
    pub fn new(_inner_data: HashMap<String, String>) -> Self {
        UnknowStrategyEvent {
            _inner_data
        }
    }
}
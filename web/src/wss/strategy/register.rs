use std::collections::HashMap;
use crate::web_error::WResult;
use crate::wss::strategy::WSSStrategy;

pub struct RegisterStrategyEvent {
    _inner_data: HashMap<String, String>
}

impl WSSStrategy for RegisterStrategyEvent {
    fn action(&self) -> WResult<Option<String>> {
        Ok(None)
    }

    fn code(&self) -> i16 {
        1
    }
}

impl RegisterStrategyEvent {
    pub fn new(_inner_data: HashMap<String, String>) -> Self {
        RegisterStrategyEvent {
            _inner_data
        }
    }
}
use crate::web_error::WResult;
use crate::wss::strategy::WSSStrategy;

pub struct IgnoreStrategyEvent {}

impl WSSStrategy for IgnoreStrategyEvent {
    fn action(&self) -> WResult<Option<String>> {
        Ok(Some("Success".to_string()))
    }

    fn code(&self) -> i16 {
        0
    }
}

impl IgnoreStrategyEvent {
    pub fn new() -> Self {
        IgnoreStrategyEvent {}
    }
}
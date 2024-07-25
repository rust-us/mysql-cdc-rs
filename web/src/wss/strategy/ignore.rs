use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::web_error::WResult;
use crate::wss::strategy::WSSStrategy;

pub struct IgnoreStrategyEvent {}

impl WSSStrategy for IgnoreStrategyEvent {
    fn action(&mut self, rt: Arc<Runtime>) -> WResult<Option<String>> {
        Ok(Some("Success[I]".to_string()))
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
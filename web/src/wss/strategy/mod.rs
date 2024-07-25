use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::web_error::WResult;

pub mod register;
pub mod factory;
mod unknow;
mod ignore;

pub trait WSSStrategy {
    fn action(&mut self, rt: Arc<Runtime>) -> WResult<Option<String>>;

    fn code(&self) -> i16;
}

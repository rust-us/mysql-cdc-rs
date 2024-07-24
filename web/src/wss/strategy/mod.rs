use std::collections::HashMap;
use crate::web_error::WResult;

pub mod register;
pub mod factory;
mod unknow;
mod ignore;

pub trait WSSStrategy {
    fn action(&self) -> WResult<Option<String>>;

    fn code(&self) -> i16;
}
use std::fmt::Debug;
use common::err::decode_error::ReError;
use crate::events::log_context::{LogContextRef};

pub trait BinlogReader<S, ITEM> {
    fn new(context: LogContextRef, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized;

    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, LogContextRef), ReError> where Self: Sized;

    fn read_events(&mut self, stream: S) -> Box<dyn Iterator<Item=Result<ITEM, ReError>>>;

    /// 获取  LogContext 上下文
    fn get_context(&self) -> LogContextRef;
}

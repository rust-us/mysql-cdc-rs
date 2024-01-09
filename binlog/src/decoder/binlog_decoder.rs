use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;
use common::err::DecodeError::ReError;
use crate::events::log_context::LogContext;

pub const PAYLOAD_BUFFER_SIZE: usize = 32 * 1024;

pub trait BinlogReader<S> {
    fn new(context: Rc<RefCell<LogContext>>, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized;

    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, Rc<RefCell<LogContext>>), ReError> where Self: Sized;

    fn read_events(self, stream: S) -> Self;

    /// 获取  LogContext 上下文
    fn get_context(&self) -> Rc<RefCell<LogContext>>;
}

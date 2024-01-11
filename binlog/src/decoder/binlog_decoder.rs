use std::fmt::Debug;
use common::err::DecodeError::ReError;
use crate::events::log_context::{LogContextRef};

pub const PAYLOAD_BUFFER_SIZE: usize = 32 * 1024;

pub trait BinlogReader<S> {
    fn new(context: LogContextRef, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized;

    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, LogContextRef), ReError> where Self: Sized;

    fn read_events(self, stream: S) -> Self;

    /// 获取  LogContext 上下文
    fn get_context(&self) -> LogContextRef;
}

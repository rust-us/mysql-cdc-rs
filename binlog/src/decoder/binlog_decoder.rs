use std::cell::RefCell;
use std::fmt::Debug;
use std::fs::File;
use std::io::{ErrorKind, IsTerminal, Read, Seek};
use std::mem::ManuallyDrop;
use std::ptr;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::vec::IntoIter;
use common::err::DecodeError::ReError;
use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_factory::EventFactory;
use crate::events::event_header::{Header, HEADER_LEN};
use crate::events::log_context::LogContext;
use crate::events::log_position::LogPosition;
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

pub const PAYLOAD_BUFFER_SIZE: usize = 32 * 1024;

pub trait BinlogReader<S> {
    fn new(context: Rc<RefCell<LogContext>>, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized;

    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, Rc<RefCell<LogContext>>), ReError> where Self: Sized;

    fn read_events(self, stream: S) -> Self;

    /// 获取  LogContext 上下文
    fn get_context(&self) -> Rc<RefCell<LogContext>>;
}

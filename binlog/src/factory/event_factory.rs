use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use bytes::Buf;
use tracing::{debug, instrument};
use common::err::DecodeError::{Needed, ReError};
use crate::decoder::binlog_decoder::BinlogReader;
use crate::decoder::bytes_binlog_reader::BytesBinlogReader;

use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_raw::EventRaw;
use crate::events::gtid_set::MysqlGTIDSet;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::log_position::LogPosition;

pub trait IEventFactory {
    /// 初始化 binlog 解析器
    fn new(skip_magic_buffer: bool) -> EventFactory;

    fn new_with_gtid_set(skip_magic_buffer: bool, gtid_set: MysqlGTIDSet) -> EventFactory;

    fn dump();

    /// 从 bytes 读取 binlog
    ///
    /// # Arguments
    ///
    /// * `input`:
    /// * `skip_magic_buffer`:  是否跳过magic_number. true 表明已经跳过了（也就是说bytes中不存在magic_buffer）。 false指仍需执行 magic_number校验
    ///
    ///                 剩余字节
    /// returns: Result<(&[u8], Vec<Event>), ReError>
    ///
    fn parser_bytes(&mut self, input: &[u8], options: &EventFactoryOption) -> Result<(Vec<u8>, Vec<Event>), ReError>;

    /// 从 Iterator 读取 binlog
    fn parser_iter(&mut self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>, options: &EventFactoryOption);

    /// 得到 EventFactory 实例后， BinlogReader 的上下文信息
    fn get_context(&self) -> LogContextRef;
}

#[derive(Debug)]
pub struct EventFactory {
    reader: BytesBinlogReader,

    context: LogContextRef,
}

impl IEventFactory for EventFactory {
    fn new(skip_magic_buffer: bool) -> EventFactory {
        EventFactory::_new(skip_magic_buffer, None)
    }

    fn new_with_gtid_set(skip_magic_buffer: bool, gtid_set: MysqlGTIDSet) -> EventFactory {
        EventFactory::_new(skip_magic_buffer, Some(gtid_set))
    }

    fn dump() {
        todo!()
    }

    #[instrument]
    fn parser_bytes(&mut self, input: &[u8], options: &EventFactoryOption) -> Result<(Vec<u8>, Vec<Event>), ReError> {
        let context = &self.context;

        let iter = self.reader.read_events(input);
        let remaing_bytes = self.reader.get_source_bytes();

        let mut events = Vec::new();
        for result in iter.into_iter() {
            let e = result.unwrap();

            if options.is_debug() {
                debug!("event: {}, process_count: {:?}", Event::get_type_name(&e),
                     context.borrow().log_stat_process_count());
            }

            events.push(e);
        }

        // 取出剩余字节
        let remaing = Vec::from(remaing_bytes.as_slice());
        Ok((remaing, events))
    }

    fn parser_iter(&mut self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>, options: &EventFactoryOption) {
        let mut remaing = Vec::new();

        for item in iter {
            match item {
                Ok(bytes) => {
                    let mut parser_bytes = Vec::<u8>::new();
                    if remaing.len() > 0 {
                        parser_bytes.extend(&remaing);
                        remaing.clear();
                    }
                    parser_bytes.extend(bytes);

                    let rs = self.parser_bytes(&parser_bytes, options);
                    if rs.is_ok() {
                        let (mut r, event_list) = rs.unwrap();
                        if r.len() > 0 {
                            remaing.append(&mut r);
                        }

                        // event_list 异步往下走， 所有权往下移。
                        if !event_list.is_empty() {
                            if options.is_debug() {
                                debug!("event_list: {:?}", event_list);
                            }

                            // todo
                        }
                    } else {
                        println!("binlog parser error");

                        // todo
                        break;
                    }
                },
                Err(e) => {
                    println!("iter get error");

                    // todo
                    break;
                },
            };
        }
    }

    // dump
    fn get_context(&self) -> LogContextRef {
        self.context.clone()
    }
}

impl EventFactory {

    fn _new(skip_magic_buffer: bool, gtid_set: Option<MysqlGTIDSet>) -> EventFactory {

        let _context:LogContext = if gtid_set.is_some() {
            // 将gtid传输至context中，供decode使用
            LogContext::new_with_gtid(LogPosition::new("BytesBinlogReader"), gtid_set.unwrap())
        } else {
            LogContext::new(LogPosition::new("BytesBinlogReader"))
        };

        let context = Rc::new(RefCell::new(_context));

        let reader = BytesBinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        EventFactory {
            reader,
            context
        }
    }

    ///EventRaw 转为 Event
    pub fn event_raw_to_event(raw: &EventRaw, context: LogContextRef) -> Result<Event, ReError> {
        let mut decoder = LogEventDecoder::new();

        decoder.decode_with_raw(&raw, context)
    }
}

#[derive(Debug)]
pub struct EventFactoryOption {
    /// 是否为 debug。 true 为阻debug模式，  false 为正常模式
    debug: bool,

    /// 是否为阻塞式。 true 为阻塞， false 为非阻塞
    blocked: bool,
}

impl EventFactoryOption {
    pub fn new(debug: bool, blocked: bool,) -> Self {
        EventFactoryOption {
            debug,
            blocked,
        }
    }

    pub fn debug() -> Self {
        EventFactoryOption ::new(true, false)
    }

    pub fn blocked() -> Self {
        EventFactoryOption ::new(false, true)
    }
}

impl Default for EventFactoryOption {
    fn default() -> Self {
        EventFactoryOption::new(false, false)
    }
}

impl EventFactoryOption {
    pub fn is_debug(&self) -> bool {
        self.debug
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
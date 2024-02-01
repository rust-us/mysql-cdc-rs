use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use bytes::Buf;
use tracing::{debug, instrument};
use common::binlog::PAYLOAD_BUFFER_SIZE;
use common::err::decode_error::{ReError};
use crate::alias::mysql::gtid::gtid_set::GtidSet;
use crate::decoder::binlog_decoder::BinlogReader;
use crate::decoder::bytes_binlog_reader::BytesBinlogReader;

use crate::decoder::event_decoder::{LogEventDecoder};
use crate::events::binlog_event::BinlogEvent;
use crate::events::event_raw::EventRaw;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::log_position::LogPosition;

pub trait IEventFactory {
    /// 初始化 binlog 解析器
    fn new(skip_magic_buffer: bool) -> EventFactory;

    fn new_with_gtid_set(skip_magic_buffer: bool, gtid_set: GtidSet) -> EventFactory;

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
    fn parser_bytes(&mut self, input: &[u8], options: &EventReaderOption) -> Result<(Vec<u8>, Vec<BinlogEvent>), ReError>;

    /// 从 Iterator 读取 binlog
    fn parser_iter(&mut self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>, options: &EventReaderOption);

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

    fn new_with_gtid_set(skip_magic_buffer: bool, gtid_set: GtidSet) -> EventFactory {
        EventFactory::_new(skip_magic_buffer, Some(gtid_set))
    }

    #[instrument]
    fn parser_bytes(&mut self, input: &[u8], options: &EventReaderOption) -> Result<(Vec<u8>, Vec<BinlogEvent>), ReError> {
        EventFactory::print_env(options);

        let context = &self.context;

        let iter = self.reader.read_events(input);
        let remaing_bytes = self.reader.get_source_bytes();

        let mut events = Vec::new();
        for result in iter.into_iter() {
            let e = result.unwrap();

            if options.is_debug() {
                let event_type = BinlogEvent::get_type_name(&e);
                let count = context.borrow().get_log_stat_process_count();
                debug!("event: {:?}, process_count: {:?}", event_type, count);
            }

            events.push(e);
        }

        // 取出剩余字节
        let remaing = Vec::from(remaing_bytes.as_slice());
        Ok((remaing, events))
    }

    // #[instrument]
    fn parser_iter(&mut self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>, options: &EventReaderOption) {
        EventFactory::print_env(options);

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
                        if !event_list.is_empty()    {
                            if options.is_debug() {
                                debug!("\n{:?}", event_list);
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

    fn _new(skip_magic_buffer: bool, gtid_set: Option<GtidSet>) -> EventFactory {

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

    fn print_env(options: &EventReaderOption) {

    }
}

impl Default for EventFactory {
    fn default() -> Self {
        EventFactory::new(true)
    }
}

#[derive(Debug, Clone)]
pub struct EventReaderOption {
    /// 是否为 debug。 true 为阻debug模式，  false 为正常模式
    debug: bool,

    /// 是否为阻塞式。 true 为阻塞， false 为非阻塞
    blocked: bool,

    payload_buffer_size: usize,
}

impl EventReaderOption {
    pub fn new(debug: bool, blocked: bool, payload_buffer_size: usize) -> Self {
        EventReaderOption {
            debug,
            blocked,
            payload_buffer_size,
        }
    }

    pub fn debug() -> Self {
        EventReaderOption::new(true, false, PAYLOAD_BUFFER_SIZE as usize)
    }

    pub fn debug_with_payload_buffer_size(payload_buffer_size: usize) -> Self {
        EventReaderOption::new(true, false, payload_buffer_size)
    }
}

impl Default for EventReaderOption {
    fn default() -> Self {
        EventReaderOption::new(false, false, PAYLOAD_BUFFER_SIZE as usize)
    }
}

impl EventReaderOption {
    pub fn is_debug(&self) -> bool {
        self.debug
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }

    pub fn get_payload_buffer_size(&self) -> usize {
        self.payload_buffer_size
    }
}

#[cfg(test)]
mod test {
    use common::binlog::column::column_type::SrcColumnType;

    #[test]
    fn test() {
        assert_eq!(1, 1);

        let dd = SrcColumnType::Geometry;
        let c = dd.clone() as u8;
        assert_eq!(255, c);
    }
}
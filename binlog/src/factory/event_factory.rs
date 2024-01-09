use std::cell::RefCell;
use std::io::Cursor;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use bytes::Buf;
use nom::IResult;
use nom::bytes::complete::take;
use nom::combinator::map;
use common::err::DecodeError::ReError;
use crate::decoder::binlog_decoder::BinlogReader;
use crate::decoder::bytes_binlog_reader::BytesBinlogReader;

use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_raw::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext};
use crate::events::log_position::LogPosition;

pub trait IEventFactory {
    /// 初始化 binlog 解析器
    fn new(skip_magic_buffer: bool) -> EventFactory;


    /// 得到 EventFactory 实例后， BinlogReader 的上下文信息
    fn get_context(&self) -> Rc<RefCell<LogContext>>;


    /// 从 bytes 读取 binlog
    ///
    /// # Arguments
    ///
    /// * `input`:
    /// * `skip_magic_buffer`:  是否跳过magic_number. true 表明已经跳过了（也就是说bytes中不存在magic_buffer）。 false指仍需执行 magic_number校验
    ///
    /// returns: Result<(&[u8], Vec<Event, Global>), Err<Error<&[u8]>>>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn parser_bytes<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Vec<Event>>;

    /// 从 Iterator 读取 binlog
    fn parser_iter(&self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>);

    /// 从 BlockIterator 读取 binlog
    fn parser_iter_with_block(&self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>);
}

pub struct EventFactory {
    reader: Arc<RwLock<BytesBinlogReader>>,

    context: Rc<RefCell<LogContext>>,
}

impl IEventFactory for EventFactory {
    fn new(skip_magic_buffer: bool) -> EventFactory {
        let _context:LogContext = LogContext::new(LogPosition::new("BytesBinlogReader"));
        let context = Rc::new(RefCell::new(_context));

        let reader = BytesBinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        EventFactory {
            reader: Arc::new(RwLock::new(reader)),
            context
        }
    }

    fn get_context(&self) -> Rc<RefCell<LogContext>> {
        self.context.clone()
    }

    fn parser_bytes<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Vec<Event>> {
        let mut reader = self.reader.write().unwrap();
        let context = &self.context;

        let iter = reader.clone().read_events(input);
        let remaing_bytes = &iter.get_source_bytes();

        let mut events = Vec::new();
        for result in iter {
            let e = result.unwrap();

            println!("============================ {}, process_count:{}.", Event::get_type_name(&e),
                     context.borrow().log_stat_process_count());
            events.push(e);
        }

        // 取出剩余字节
        let rm = if remaing_bytes.len() != 0 {
            &input[remaing_bytes.len()..input.len()]
        } else {
            let (i, bytes) = map(take(input.len()), |s: &[u8]| s)(input)?;
            i
        };

        Ok((rm, events))
    }

    fn parser_iter(&self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>) {
        for i in iter {
            let bytes = match i {
                Ok(bytes) => {
                    // bytes
                    println!("get: {:?}", bytes);

                    self.parser_bytes(&*bytes).expect("TODO: panic message");
                },
                Err(e) => {
                    println!("error");
                    break;
                },
            };
        }
    }

    fn parser_iter_with_block(&self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>) {
        for i in iter {
            let bytes = match i {
                Ok(bytes) => {
                    self.parser_bytes(&*bytes).expect("TODO: panic message");
                },
                Err(e) => {
                    println!("error");
                    break;
                },
            };
        }
    }
}

impl EventFactory {

    ///EventRaw 转为 Event
    pub fn event_raw_to_event(raw: &EventRaw, context: Rc<RefCell<LogContext>>) -> Result<Event, ReError> {
        let mut decoder = LogEventDecoder::new();
        let rs = decoder.decode_with_raw(&raw, context);

        match rs {
            Err(e) => {
                // 中途的解析错误暂时忽略。后续再处理
                // todo
                println!("中途的解析错误暂时忽略。后续再处理： {:?}", e);
                Err(ReError::Error(String::from("中途的解析错误暂时忽略。后续再处理")))
            },
            Ok(e) => {
                assert_eq!(e.1.len(), 0);
                Ok(e.0)
            }
        }
    }
}


#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
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
use crate::events::event_c::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::log_position::LogPosition;

pub struct EventFactory {
    reader: Arc<RwLock<BytesBinlogReader>>,

    context: Rc<RefCell<LogContext>>,
}

impl EventFactory {
    pub fn get_context(&self) -> Rc<RefCell<LogContext>> {
        self.context.clone()
    }

    pub fn new(skip_magic_buffer: bool) -> Self {
        let _context:LogContext = LogContext::new(LogPosition::new("BytesBinlogReader"));
        let context = Rc::new(RefCell::new(_context));

        let reader = BytesBinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        EventFactory {
            reader: Arc::new(RwLock::new(reader)),
            context
        }
    }

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
    pub fn parser_bytes<'a>(&self, input: &'a [u8]) -> IResult<&'a [u8], Vec<Event>> {
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

    /// 从 Iterator 读取 binlog
    pub fn parser_iter(&self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>) {
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

    /// 从 BlockIterator 读取 binlog
    pub fn parser_iter_with_block(&self, iter: impl Iterator<Item=Result<Vec<u8>, impl Into<ReError>>>) {
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

    /// input &[u8] 转为 Vec<EventRaw>， 并返回剩余数组
    pub fn steam_to_event_raw<'a>(input: &'a [u8], context: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Vec<EventRaw>> {
        let header_len = context.borrow_mut().get_format_description().common_header_len as usize;
        let mut event_raws = Vec::<EventRaw>::new();

        if input.len() < header_len {
            return Ok((input, event_raws));
        }

        let mut i1 : &[u8] = input;
        loop {
            if i1.len() < header_len {
                return Ok((i1, event_raws));
            }

            // let (raw, remaining) = EventFactory::slice(i1, header_len);
            let (remaining, raw) = EventFactory::popup(i1, header_len)?;

            event_raws.push(raw);
            i1 = remaining;
        }
        // loop end
    }

    /// 提前计算crc的slice
    fn slice(slice: &[u8], header_len: usize) -> (EventRaw, &[u8]) {
        let mut i = slice;

        // try parser
        let header_bytes = &i[0..header_len];
        let (_i, mut header) = Header::parse_v4_header(header_bytes).unwrap();
        assert_eq!(_i.len(), 0);
        let event_len = header.event_length as usize;

        let payload_data = &i[header_len..event_len];

        let payload_data_without_crc_bytes = &payload_data[0..payload_data.len()-4];
        let crc_bytes = &payload_data[payload_data.len()-4..payload_data.len()];

        let mut cursor = Cursor::new(crc_bytes);
        let checksum = cursor.get_u32_le();
        header.set_checksum(checksum);

        let raw = EventRaw::new_with_payload(header, payload_data_without_crc_bytes.to_vec(), false);

        (raw, &i[event_len..])
    }

    /// 不提前计算crc的popup
    fn popup<'a>(slice: &'a [u8], header_len: usize) -> IResult<&'a [u8], EventRaw> {
        let (i, header) = Header::parse_v4_header(slice)?;
        let payload_len = header.event_length - header_len as u32;
        let (i, payload_data) = map(take(payload_len), |s: &[u8]| s.to_vec())(i)?;

        let raw = EventRaw::new_with_payload(header, payload_data, true);

        Ok((i, raw))
    }

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
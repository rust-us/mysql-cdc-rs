use std::cell::RefCell;
use std::rc::Rc;
use nom::IResult;
use nom::multi::many1;
use nom::bytes::complete::take;
use nom::combinator::map;
use common::err::DecodeError::ReError;

use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;

pub struct EventFactory {
    //
    bytes: Vec<u8>,
}

impl EventFactory {

    /// 接口作废
    pub fn from_bytes<'a>(input: &'a [u8]) -> IResult<&'a [u8], Vec<Event>> {
        let (i, _) = Header::check_start(input)?;

        let rs = many1(Event::parse)(i);
        rs
    }

    /// input &[u8] 转为 Vec<EventRaw>， 并返回剩余数组
    pub fn steam_to_event_raw<'a>(input: &'a [u8], context: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Vec<EventRaw>> {
        let header_len = context.borrow_mut().clone().get_format_description().common_header_len as usize;
        let mut event_raws = Vec::<EventRaw>::new();

        if input.len() < header_len {
            return Ok((input, event_raws));
        }

        let mut i1 : &[u8] = input;
        loop {
            if i1.len() < header_len {
                return Ok((i1, event_raws));
            }

            // try parser
            let (i, header) = Header::parse_v4_header(i1)?;
            let payload_len = header.event_length - header_len as u32;
            let (i, payload_data) = map(take(payload_len), |s: &[u8]| s.to_vec())(i)?;

            let raw = EventRaw::new_with_payload(header, payload_data);
            event_raws.push(raw);

            i1 = i;
        }
        // loop end
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
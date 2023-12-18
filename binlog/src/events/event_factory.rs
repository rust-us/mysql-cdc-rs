use std::cell::RefCell;
use std::rc::Rc;
use nom::IResult;
use nom::multi::many1;
use nom::bytes::complete::take;
use nom::combinator::map;

use crate::decoder::log_decoder::{LogDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::log_position::LogPosition;
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

pub struct EventFactory {

}

impl EventFactory {

    pub fn from_bytes<'a>(input: &'a [u8]) -> IResult<&'a [u8], Vec<Event>> {
        let (i, _) = Header::check_start(input)?;

        let rs = many1(Event::parse)(i);
        rs
    }

    pub fn from_bytes_with_context<'a>(bytes: &'a [u8]) -> IResult<&'a [u8], Vec<Event>> {
        let (i, _) = Header::check_start(bytes)?;

        let mut context:LogContext = LogContext::default();
        &context.set_log_position(LogPosition::new("test".to_string()));
        let context_ref = Rc::new(RefCell::new(context));

        // try parser first header
        let (i, event_raws) = EventFactory::assembly_event_raw(i, Rc::clone(&context_ref))?;

        let mut event_list = Vec::<Event>::with_capacity(event_raws.len());
        for event_raw in event_raws {
            let rs = LogEventDecoder::decode(&event_raw, Rc::clone(&context_ref));

            match rs {
                Err(e) => {
                    // 中途的解析错误暂时忽略。后续再处理
                    // todo
                    println!("中途的解析错误暂时忽略。后续再处理： {:?}", e);
                },
                Ok(e) => {
                    assert_eq!(e.remain_bytes.len(), 0);
                    event_list.push(e.event);
                }
            }
        }

        Ok((i, event_list))
    }

    fn assembly_event_raw<'a>(input: &'a [u8], context_ref: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Vec<EventRaw>> {
        let mut event_raws = Vec::<EventRaw>::new();

        if input.len() < context_ref.borrow_mut().clone().get_format_description().common_header_len as usize {
            return Ok((input, event_raws));
        }

        let mut i_1 : &[u8] = input;
        loop {
            if i_1.len() < context_ref.borrow_mut().clone().get_format_description().common_header_len as usize {
                return Ok((i_1, event_raws));
            }

            // try parser
            let (i, header) = Header::parse_v4_header(i_1)?;
            let payload_len = header.event_length - LOG_EVENT_HEADER_LEN as u32;
            let (i, payload_data) = map(take(payload_len), |s: &[u8]| s.to_vec())(i)?;

            let raw = EventRaw::new_with_payload(header, payload_data);
            event_raws.push(raw);

            i_1 = i;
        }
        // loop end
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
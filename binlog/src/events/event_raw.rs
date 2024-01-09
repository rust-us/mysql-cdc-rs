use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;
use bytes::Buf;
use nom::bytes::complete::take;
use nom::combinator::map;
use nom::IResult;
use serde::Serialize;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext};
use crate::events::protocol::format_description_log_event::{LOG_EVENT_MINIMAL_HEADER_LEN, ST_COMMON_HEADER_LEN_OFFSET};
use crate::factory::event_factory::EventFactory;

/////////////////////////////////////
///  Event Data
/////////////////////////////////////
#[derive(Debug, Serialize, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct EventRaw {
    pub header: Header,

    // payload_data_without_crc
    pub payload: Vec<u8>,

    /// payload 中是否包含crc信息。 false 不包含
    pub has_crc: bool,
}

impl EventRaw {
    pub fn new(header: Header) -> Self {
        EventRaw {
            header,
            payload: Vec::with_capacity(32),
            has_crc: false,
        }
    }

    pub fn new_with_payload(header: Header, payload: Vec<u8>, has_crc: bool) -> Self {
        EventRaw {
            header,
            payload,
            has_crc,
        }
    }

    pub fn get_header(&self) -> &Header {
        &self.header
    }

    pub fn get_header_ref(&self) -> Rc<&Header> {
        Rc::new(&self.header)
    }

    pub fn get_payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

impl EventRaw {

    /// input &[u8] 转为 Vec<EventRaw>， 并返回剩余数组
    pub fn steam_to_event_raw<'a>(input: &'a [u8], context: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], Vec<EventRaw>> {
        let header_len = context.borrow_mut().get_format_description().common_header_len as usize;
        let mut event_raws = Vec::<EventRaw>::new();

        if input.len() < header_len {
            return Ok((input, event_raws));
        }

        let mut bytes : &[u8] = input;
        loop {
            if bytes.len() < header_len {
                return Ok((bytes, event_raws));
            }

            // let (raw, remaining) = EventFactory::slice(i1, header_len);
            let (remaining, raw) = EventRaw::popup(bytes, header_len, context.clone())?;

            event_raws.push(raw);
            bytes = remaining;
        }
        // loop end
    }

    /// 提前计算crc的slice
    fn slice(slice: &[u8], header_len: usize, context: Rc<RefCell<LogContext>>) -> (EventRaw, &[u8]) {
        let mut i = slice;

        // try parser
        let header_bytes = &i[0..header_len];
        let mut header = Header::parse_v4_header(header_bytes, context.clone()).unwrap();
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
    fn popup<'a>(bytes: &'a [u8], header_len: usize, context: Rc<RefCell<LogContext>>) -> IResult<&'a [u8], EventRaw> {
        let header_bytes = &bytes[0..header_len];

        let header = Header::parse_v4_header(header_bytes, context.clone()).unwrap();
        let event_len = header.event_length;

        let remaining = &bytes[header_len..];
        let payload_len = event_len - header_len as u32;
        let (remaining, payload_data) = map(take(payload_len), |s: &[u8]| s.to_vec())(remaining)?;

        let raw = EventRaw::new_with_payload(header, payload_data, true);

        Ok((remaining, raw))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
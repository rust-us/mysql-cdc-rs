use std::cell::RefCell;
use std::rc::Rc;
use bytes::Buf;
use common::err::decode_error::{Needed, ReError};
use common::err::decode_error::ReError::Incomplete;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContextRef};

pub type HeaderRef = Rc<RefCell<Header>>;

/////////////////////////////////////
///  Event Data
/////////////////////////////////////
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct EventRaw {
    pub header: HeaderRef,

    // payload_data_without_crc
    pub payload: Vec<u8>,

    /// payload 中是否包含crc信息。 false 不包含
    pub has_crc: bool,
}

impl EventRaw {
    pub fn new(header: Header) -> Self {
        EventRaw::new_with_payload(header, Vec::with_capacity(32))
    }

    pub fn new_with_payload(header: Header, payload: Vec<u8>) -> Self {
        EventRaw::new_with_payload_crc(header, payload, false)
    }

    pub fn new_with_payload_crc(header: Header, payload: Vec<u8>, has_crc: bool) -> Self {
        EventRaw {
            header: Rc::new(RefCell::new(header)),
            payload,
            has_crc,
        }
    }

    pub fn get_header(&self) -> HeaderRef {
        self.header.clone()
    }

    pub fn get_payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

impl EventRaw {

    /// input &[u8] 转为 Vec<EventRaw>， 并返回剩余数组
    pub fn steam_to_event_raw(input: &[u8], context: LogContextRef) -> Result<(Vec<u8>, Vec<EventRaw>), ReError> {
        let header_len = context.borrow_mut().get_format_description().common_header_len as usize;
        let mut event_raws = Vec::<EventRaw>::new();

        if input.len() < header_len {
            return Ok((Vec::from(input), event_raws));
        }

        let mut bytes : Vec<u8> = Vec::from(input);
        let mut no_enough_data = false;
        loop {
            if no_enough_data || (bytes.len() < header_len) {
                return Ok((bytes, event_raws));
            }

            let rs = EventRaw::popup(bytes.as_slice(), header_len, context.clone());

            match rs {
                Ok((remaining, raw)) => {
                    event_raws.push(raw);
                    bytes = remaining;
                },
                Err(e) => {
                    match &e {
                        Incomplete(need) => {
                            match need {
                                Needed::NoEnoughData => {
                                    no_enough_data = true;
                                },
                                _ => {
                                    return Err(e);
                                }
                            }
                        },
                        _ => {
                            return Err(e)
                        }
                    };
                }
            };
        }
        // loop end
    }

    /// 不提前计算crc的popup
    fn popup(bytes: &[u8], header_len: usize, context: LogContextRef) -> Result<(Vec<u8>, EventRaw), ReError> {
        let header_bytes = &bytes[0..header_len];

        let header = Header::parse_v4_header(header_bytes, context.clone()).unwrap();
        let event_len = header.event_length;

        if bytes.len() < event_len as usize {
            return Err(ReError::Incomplete(Needed::NoEnoughData));
        }

        let remaining: &[u8] = &bytes[header_len..];
        let payload_len = event_len - header_len as u32;

        let payload_data = &remaining[0..(payload_len as usize)];
        let remained = Vec::from(&remaining[(payload_len as usize)..]);

        let raw = EventRaw::new_with_payload_crc(header, payload_data.to_vec(), true);

        Ok((remained, raw))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
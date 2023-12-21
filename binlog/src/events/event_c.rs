use std::rc::Rc;
use crate::events::event_header::Header;

/////////////////////////////////////
///  Event Data
/////////////////////////////////////
#[derive(Debug, Clone)]
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

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
use std::rc::Rc;
use crate::events::event_header::Header;

/////////////////////////////////////
///  Event Data
/////////////////////////////////////
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct EventRaw {
    pub header: Header,

    pub payload: Vec<u8>,
}

impl EventRaw {
    pub fn new(header: Header) -> Self {
        EventRaw {
            header,
            payload: Vec::with_capacity(32),
        }
    }

    pub fn new_with_payload(header: Header, payload: Vec<u8>) -> Self {
        EventRaw {
            header,
            payload,
        }
    }

    pub fn get_header(&self) -> Rc<&Header> {
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
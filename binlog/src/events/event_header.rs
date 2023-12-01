use serde::Serialize;
use crate::events::event_header_flag::EventFlag;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Header {
    pub timestamp: u32,
    pub event_type: u8,
    pub server_id: u32,
    pub event_size: u32,
    pub log_pos: u32,
    pub flags: EventFlag,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}

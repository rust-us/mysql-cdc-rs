use crate::event_header::EventHeader;

/////////////////////////////////////
///  Event Data
/////////////////////////////////////
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct EventRaw {
    pub header: EventHeader,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
pub struct Event<P> {
    pub header: EventHeader,
    pub payload: P,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
use std::collections::HashMap;
use crate::events::checksum_type::ST_COMMON_PAYLOAD_CHECKSUM_LEN;
use crate::events::event::Event;
use crate::events::event_header::Header;
use crate::events::log_event::LogEvent;
use crate::events::protocol::format_description_log_event::LOG_EVENT_MINIMAL_HEADER_LEN;
use nom::{bytes::complete::take, combinator::map, number::complete::le_u32, IResult};
use serde::Serialize;

/// source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L1073-L1103
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct PreviousGtidsLogEvent {
    header: Header,

    /// field buf,  It contains the Gtids executed in the last binary log file.
    pub gtid_sets: Vec<u8>,

    /// Contains the serialized event（序列化的事件）
    buf_size: u32,
}

impl PreviousGtidsLogEvent {
    pub fn parse<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], PreviousGtidsLogEvent> {
        let gtid_sets_len = header.event_length
            - (LOG_EVENT_MINIMAL_HEADER_LEN + /*buf_size len*/4 + /*checksum len*/ST_COMMON_PAYLOAD_CHECKSUM_LEN)
                as u32;
        let (i, gtid_sets) = map(take(gtid_sets_len), |s: &[u8]| s.to_vec())(input)?;

        let (i, buf_size) = le_u32(i)?;

        let (i, checksum) = le_u32(i)?;
        let header_new = Header::copy_and_get(&header, checksum, HashMap::new());

        Ok((
            i,
            PreviousGtidsLogEvent {
                header: header_new,

                gtid_sets,
                buf_size,
            },
        ))
    }
}

impl LogEvent for PreviousGtidsLogEvent {
    fn get_type_name(&self) -> String {
        "PreviousGtidsLogEvent".to_string()
    }
}

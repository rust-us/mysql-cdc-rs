use std::collections::HashMap;
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::decoder::event_parser_dispatcher::event_parse_diapatcher;
use crate::events::checksum_type::ChecksumType;

use crate::events::event::Event;
use crate::events::event_raw::{EventRaw, HeaderRef};
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::protocol::table_map_event::TableMapEvent;

pub trait EventDecoder {

    ///
    ///
    /// # Arguments
    ///
    /// * `raw`:  解析的字节码
    /// * `context`:
    ///
    /// returns: Result<(Event, Vec<u8, Global>), ReError>
    ///             Event 解析事件
    ///             &[u8]  剩余的未解析字节码
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn decode_with_raw(&mut self, raw: &EventRaw, context: LogContextRef) -> Result<(Event, Vec<u8>), ReError>;

    ///
    ///
    /// # Arguments
    ///
    /// * `slice`:  解析的字节码
    /// * `header`:
    /// * `context`:
    ///
    /// returns: Result<(Event, Vec<u8, Global>), ReError>
    ///             Event 解析事件
    ///             &[u8]  剩余的未解析字节码
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn decode_with_slice(&mut self, slice: &[u8], header: HeaderRef, context: LogContextRef) -> Result<(Event, Vec<u8>), ReError>;
}

#[derive(Debug, Serialize, Clone)]
pub struct LogEventDecoder {
    /// Gets checksum algorithm type used in a binlog file.
    pub checksum_type: ChecksumType,

    /// Gets TableMapEvent cache required in row events.
    pub table_map: HashMap<u64, TableMapEvent>,
}

impl EventDecoder for LogEventDecoder {
    fn decode_with_raw(&mut self, raw: &EventRaw, context: LogContextRef) -> Result<(Event, Vec<u8>), ReError> {
        let header = raw.get_header();
        let slice = raw.get_payload();

        self.decode_with_slice(slice, header, context)
    }

    fn decode_with_slice(&mut self, slice: &[u8], header: HeaderRef, context: LogContextRef) -> Result<(Event, Vec<u8>), ReError> {
         match event_parse_diapatcher(self, slice, header, context) {
            Err(e) => return Err(ReError::Error(e.to_string())),
            Ok((i1, o)) => {
                Ok((o, i1.to_vec()))
            }
        }
    }
}

impl LogEventDecoder {
    pub fn new() -> Self {
        Self {
            checksum_type: ChecksumType::None,
            table_map: HashMap::new(),
        }
    }

    pub fn new_with_checksum_type(checksum_type: ChecksumType) -> Self {
        Self {
            checksum_type,
            table_map: HashMap::new(),
        }
    }
}

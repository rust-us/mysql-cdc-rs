use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use binlog::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use binlog::events::checksum_type::ChecksumType;
use binlog::events::event::Event;
use binlog::events::event_header::Header;
use binlog::events::event_raw::HeaderRef;
use binlog::events::log_context::{ILogContext, LogContext, LogContextRef};
use binlog::factory::event_factory::{EventReaderOption, IEventFactory};
use common::binlog::EVENT_HEADER_SIZE;
use common::err::CResult;
use common::err::decode_error::ReError;
use crate::conn::packet_channel::PacketChannel;
use crate::packet::end_of_file_packet::EndOfFilePacket;
use crate::packet::error_packet::ErrorPacket;
use crate::packet::response_type::ResponseType;

pub struct BinlogEvents {
    pub channel: Arc<RefCell<PacketChannel>>,
    pub parser: LogEventDecoder,
    pub options: EventReaderOption,
    log_context: LogContextRef,
}

impl BinlogEvents {
    pub fn new(channel: Arc<RefCell<PacketChannel>>, log_context: LogContextRef, checksum: ChecksumType) -> Self {
        let mut parser = LogEventDecoder::new();

        let options = EventReaderOption::debug();

        Self {
            channel,
            parser,
            options,
            log_context
        }
    }

    pub fn read_event(&mut self, packet: &[u8]) -> CResult<Vec<Event>> {
        let header = Header::parse_v4_header(&packet[1..], self.log_context.clone()).unwrap();
        let header_ref: HeaderRef = Rc::new(RefCell::new(header));

        let event_slice = &packet[1 + EVENT_HEADER_SIZE..];

        let event = self.parser.parse_event(event_slice, header_ref.clone(), self.log_context.clone())?;

        Ok(vec![event])
    }

    pub fn read_error(&mut self, packet: &[u8]) -> CResult<Vec<Event>> {
        let error = ErrorPacket::parse(&packet[1..])?;

        Err(ReError::String(format!("Event stream error. {:?}", error)))
    }
}

impl Default for BinlogEvents {
    fn default() -> Self {
        BinlogEvents {
            channel: Arc::new(RefCell::new(PacketChannel::default())),
            parser: LogEventDecoder::new(),
            options: EventReaderOption::default(),
            log_context: Rc::new(RefCell::new(LogContext::default())),
        }
    }
}

impl Iterator for BinlogEvents {
    type Item = CResult<Vec<Event>>;

    /// Reads binlog event packets from network stream.
    /// <a href="https://mariadb.com/kb/en/3-binlog-network-stream/">See more</a>
    fn next(&mut self) -> Option<Self::Item> {
        let (packet, _) = match self.channel.borrow_mut().read_packet() {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };

        match packet[0] {
            ResponseType::OK => {
                self.log_context.borrow_mut().update_log_stat_add();

                Some(self.read_event(&packet))
            },
            ResponseType::ERROR => Some(self.read_error(&packet)),
            ResponseType::END_OF_FILE => {
                let _ = EndOfFilePacket::parse(&packet[1..]);
                None
            }
            _ => Some(Err(ReError::String(
                "Unknown network stream status".to_string(),
            ))),
        }
    }
}

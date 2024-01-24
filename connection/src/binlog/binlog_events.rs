use binlog::events::checksum_type::ChecksumType;
use binlog::events::event::Event;
use binlog::events::event_header::Header;
use binlog::factory::event_factory::{EventFactory, EventReaderOption, IEventFactory};
use common::err::CResult;
use common::err::decode_error::ReError;
use crate::conn::packet_channel::PacketChannel;
use crate::packet::end_of_file_packet::EndOfFilePacket;
use crate::packet::error_packet::ErrorPacket;
use crate::packet::response_type::ResponseType;

pub struct BinlogEvents {
    pub channel: PacketChannel,
    pub parser: EventFactory,
    pub options: EventReaderOption,
}

impl BinlogEvents {
    pub fn new(channel: PacketChannel, checksum: ChecksumType) -> Self {
        let mut parser = EventFactory::new(true);

        let options = EventReaderOption::debug();

        Self {
            channel,
            parser,
            options }
    }

    pub fn read_event(&mut self, packet: &[u8]) -> CResult<Vec<Event>> {
        let (remaing_bytes, event) = self.parser.parser_bytes(packet, &self.options)?;

        assert_eq!(remaing_bytes.len(), 0);
        Ok(event)
    }

    pub fn read_error(&mut self, packet: &[u8]) -> CResult<Vec<Event>> {
        let error = ErrorPacket::parse(&packet[1..])?;

        Err(ReError::String(format!("Event stream error. {:?}", error)))
    }
}

impl Default for BinlogEvents {
    fn default() -> Self {
        BinlogEvents {
            channel: PacketChannel::default(),
            parser: EventFactory::default(),
            options: EventReaderOption::default(),
        }
    }
}

impl Iterator for BinlogEvents {
    type Item = CResult<Vec<Event>>;

    /// Reads binlog event packets from network stream.
    /// <a href="https://mariadb.com/kb/en/3-binlog-network-stream/">See more</a>
    fn next(&mut self) -> Option<Self::Item> {
        let (packet, _) = match self.channel.read_packet() {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };

        match packet[0] {
            ResponseType::OK => Some(self.read_event(&packet)),
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

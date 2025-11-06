use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use binlog::decoder::event_decoder::{LogEventDecoder};
use binlog::events::checksum_type::ChecksumType;
use binlog::events::binlog_event::BinlogEvent;
use binlog::events::event_header::Header;
use binlog::events::event_raw::HeaderRef;
use binlog::events::log_context::{ILogContext, LogContext, LogContextRef};
use binlog::events::log_position::LogFilePosition;
use binlog::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;
use binlog::factory::event_factory::{EventReaderOption, IEventFactory};
use common::binlog::{EVENT_HEADER_SIZE, PAYLOAD_BUFFER_SIZE};
use common::err::CResult;
use common::err::decode_error::ReError;
use crate::conn::packet_channel::PacketChannel;
use crate::packet::end_of_file_packet::EndOfFilePacket;
use crate::packet::error_packet::ErrorPacket;
use crate::packet::response_type::ResponseType;

#[derive(Debug)]
pub struct BinlogEvents {
    channel: Arc<RefCell<PacketChannel>>,
    parser: LogEventDecoder,

    options: EventReaderOption,
    log_context: LogContextRef,

    /// 加载的缓冲区。 会被多次复用
    payload_buffer: Vec<u8>,
}

impl BinlogEvents {
    pub fn new(channel: Arc<RefCell<PacketChannel>>, log_context: LogContextRef, checksum: ChecksumType,
               payload_buffer_size: usize) -> Result<Self, ReError> {
        let mut parser = LogEventDecoder::new()?;

        let options = EventReaderOption::debug_with_payload_buffer_size(payload_buffer_size);

        Ok(Self {
            channel,
            parser,
            options,
            log_context,
            payload_buffer: Vec::with_capacity(payload_buffer_size),
        })
    }

    pub fn read_event(&mut self, packet: &[u8]) -> CResult<Vec<BinlogEvent>> {
        let header = Header::parse_v4_header(&packet[1..], self.log_context.clone()).unwrap();
        let payload_length = (&header.get_event_length() - LOG_EVENT_HEADER_LEN as u32) as usize;

        let header_ref: HeaderRef = Rc::new(RefCell::new(header));

        let event = if payload_length > self.options.get_payload_buffer_size() {
            // 事件payload大小超过缓冲buffer，直接以事件payload大小分配新字节数组，用于读取事件的完整大小
            // let mut event_slice: Vec<u8> = vec![0; payload_length];
            let event_slice = &packet[1 + EVENT_HEADER_SIZE..];

            self.parser.event_parse_mergr(event_slice, header_ref.clone(), self.log_context.clone())?
        } else {
            // 从缓冲区中取空字节数组，用于读取事件的完整大小。let event_slice = &mut self.packet[0..payload_length]。
            // 此处采用了直接 slice 的切片形式。不存在内存分配。更节省内存。
            let event_slice = &packet[1 + EVENT_HEADER_SIZE..];

            self.parser.event_parse_mergr(event_slice, header_ref.clone(), self.log_context.clone())?
        };

        Ok(vec![event])
    }

    pub fn read_error(&mut self, packet: &[u8]) -> CResult<Vec<BinlogEvent>> {
        let error = ErrorPacket::parse(&packet[1..])?;

        Err(ReError::String(format!("Event stream error. {:?}", error)))
    }

    /// 获取接受到的流量总大小
    pub fn get_receives_bytes(&self) -> usize {
        self.log_context.borrow().load_receives_bytes()
    }

    /// 获取当前的 LogFilePosition
    pub fn get_log_position(&self) -> LogFilePosition {
        self.log_context.borrow().get_log_position()
    }
}

impl Clone for BinlogEvents {
    fn clone(&self) -> Self {
        BinlogEvents {
            channel: self.channel.clone(),
            parser: self.parser.clone(),
            options: self.options.clone(),
            log_context: self.log_context.clone(),
            payload_buffer: self.payload_buffer.clone(),
        }
    }
}

impl Default for BinlogEvents {
    fn default() -> Self {
        BinlogEvents {
            channel: Arc::new(RefCell::new(PacketChannel::default())),
            parser: LogEventDecoder::new().expect("Failed to create LogEventDecoder"),
            options: EventReaderOption::default(),
            log_context: Rc::new(RefCell::new(LogContext::default())),
            payload_buffer: Vec::new(),
        }
    }
}

impl Iterator for BinlogEvents {
    type Item = CResult<Vec<BinlogEvent>>;

    /// Reads binlog event packets from network stream.
    /// <a href="https://mariadb.com/kb/en/3-binlog-network-stream/">See more</a>
    fn next(&mut self) -> Option<Self::Item> {
        let (packet, _) = match self.channel.borrow_mut().read_packet() {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };

        match packet[0] {
            ResponseType::OK => {
                self.log_context.borrow_mut().add_log_stat(packet.len());

                Some(self.read_event(&packet))
            },
            ResponseType::ERROR => Some(self.read_error(&packet)),
            ResponseType::END_OF_FILE => {
                let _ = EndOfFilePacket::parse(&packet[1..]);
                None
            },
            _ => Some(Err(ReError::String(
                "Unknown network stream status".to_string(),
            ))),
        }
    }
}

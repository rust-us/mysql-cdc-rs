use std::cell::RefCell;
use std::fmt::Debug;
use std::fs::File;
use std::io::{ErrorKind, IsTerminal, Read, Seek};
use std::mem::ManuallyDrop;
use std::ptr;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::vec::IntoIter;
use common::err::DecodeError::ReError;
use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_factory::EventFactory;
use crate::events::event_header::{Header, HEADER_LEN};
use crate::events::log_context::LogContext;
use crate::events::log_position::LogPosition;
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

pub const PAYLOAD_BUFFER_SIZE: usize = 32 * 1024;

pub trait BinlogReader<S> {
    fn new(stream: S) -> Result<Self, ReError> where Self: Sized;

    fn read_events(self) -> Self;

    /// 是否读取结束
    fn eof(&self) -> bool;
}

/// Reads binlog events from a stream.
#[derive(Debug)]
pub struct FileBinlogReader {
    // todo  or Path
    stream: File,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    /// stream 与 source_bytes 加载的缓冲区。 在一次BinlogReader中会被多次复用
    payload_buffer: Vec<u8>,

    context: Rc<RefCell<LogContext>>,

    eof: bool,
}

#[derive(Debug, Clone)]
pub struct FileBinlogReaderIter {
    stream: Rc<File>,

    reader: Rc<FileBinlogReader>,
}

/// Reads binlog events from a stream.
#[derive(Debug, Clone)]
pub struct BytesBinlogReader {
    /// 源内容。在读取结束后也可能会包含读取结束时的剩余字节。用于追加下一次请求中或者直接返回
    source_bytes: Vec<u8>,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: Rc<RefCell<LogContext>>,

    event_raw_iter: Arc<IntoIter<EventRaw>>,

    eof: bool,
}

impl BinlogReader<File> for FileBinlogReader {
    fn new(mut source: File) -> Result<Self, ReError> {
        let _context:LogContext = LogContext::new(LogPosition::new("test_demo".to_string()));
        let context = Rc::new(RefCell::new(_context));

        // Parse magic
        let mut magic_buffer = vec![0; HEADER_LEN as usize];
        // read exactly HEADER_LEN bytes
        source.read_exact(&mut magic_buffer).unwrap();
        let (i, _) = Header::check_start(magic_buffer.as_slice()).unwrap();
        assert_eq!(i.len(), 0);

        Ok(Self {
            stream: source,
            decoder: LogEventDecoder::new(),
            payload_buffer: vec![0; PAYLOAD_BUFFER_SIZE],
            context,
            eof: false,
        })
    }

    fn read_events(self) -> Self {
        self
    }

    fn eof(&self) -> bool {
        self.eof || self.stream.is_terminal()
    }
}

impl FileBinlogReader {
    fn read_event(&mut self) -> Result<(Header, Event), ReError> {
        // Parse header
        let mut header_buffer = [0; LOG_EVENT_HEADER_LEN as usize];
        self.stream.read_exact(&mut header_buffer)?;
        let (i, header) = Header::parse_v4_header(&header_buffer).unwrap();
        assert_eq!(i.len(), 0);

        // parser payload
        let payload_length = header.event_length as usize - LOG_EVENT_HEADER_LEN as usize;

        if payload_length > PAYLOAD_BUFFER_SIZE {
            // 事件payload大小超过缓冲buffer，直接以事件payload大小分配新字节数组，用于读取事件的完整大小
            let mut vec: Vec<u8> = vec![0; payload_length];
            self.stream.read_exact(&mut vec)?;

            let (binlog_event, remain_bytes) = self.decoder.decode_with_slice(&vec, &header, self.context.clone()).unwrap();
            assert_eq!(remain_bytes.len(), 0);

            Ok((header, binlog_event))
        } else {
            // 从缓冲区中取空字节数组，用于读取事件的完整大小
            let slice = &mut self.payload_buffer[0..payload_length];
            self.stream.read_exact(slice)?;

            let (binlog_event, remain_bytes) = self.decoder.decode_with_slice(slice, &header, self.context.clone()).unwrap();
            assert_eq!(remain_bytes.len(), 0);

            Ok((header, binlog_event))
        }
    }
}

impl BinlogReader<&[u8]> for BytesBinlogReader {
    fn new(source: &[u8]) -> Result<Self, ReError> {
        let _context:LogContext = LogContext::new(LogPosition::new("BytesBinlogReader".to_string()));
        let context = Rc::new(RefCell::new(_context));

        let event_raw_list = Vec::new();
        Ok(Self {
            source_bytes: source.to_vec(),
            decoder: LogEventDecoder::new(),
            context,
            event_raw_iter: Arc::new(event_raw_list.clone().into_iter().clone()),
            eof: false,
        })
    }

    fn read_events(mut self) -> Self {
        let (i, _) = Header::check_start(&self.source_bytes).unwrap();
        self.source_bytes = i.to_vec();

        let (remaining_bytes, event_raws) = EventFactory::steam_to_event_raw(&self.source_bytes, self.context.clone()).unwrap();
        self.source_bytes = remaining_bytes.to_vec();
        self.event_raw_iter = Arc::new(event_raws.into_iter());

        self
    }

    fn eof(&self) -> bool {
        self.eof
    }
}

impl BytesBinlogReader {
    fn read_event(&mut self, raw: &EventRaw) -> Result<Event, ReError> {
        let (binlog_event, remain_bytes) = self.decoder.decode_with_raw(&raw, self.context.clone()).unwrap();
        assert_eq!(remain_bytes.len(), 0);

        Ok(binlog_event)
    }

    pub fn get_source_bytes(&self) -> Vec<u8> {
        self.source_bytes.clone()
    }
}

/// Iterator
impl Iterator for FileBinlogReader {
    type Item = Result<(Header, Event), ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.read_event();

        if let Err(error) = &result {
            if let ReError::IoError(io_error) = error {
                // 文件读到了最后, is IoError(Error { kind: UnexpectedEof, message: "failed to fill whole buffer" })
                if let ErrorKind::UnexpectedEof = io_error.kind() {
                    self.eof = true;
                    return None;
                } else {
                    println!("{:?}", error);
                }
            } else {
                println!("{:?}", error);
            }
        }

        Some(result)
    }
}

/// Iterator
impl Iterator for BytesBinlogReader {
    type Item = Result<Event, ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        let it = Arc::get_mut(&mut self.event_raw_iter).unwrap();
        let event_raw = it.next();

        if event_raw.is_none() {
            return None;
        }

        let result = self.read_event(&event_raw.unwrap());

        if let Err(error) = &result {
            if let ReError::IoError(io_error) = error {
                if let ErrorKind::UnexpectedEof = io_error.kind() {
                    self.eof = true;
                    return None;
                }
            }
        }

        Some(result)
    }
}

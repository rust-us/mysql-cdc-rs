use std::cell::RefCell;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::rc::Rc;
use common::err::DecodeError::{Needed, ReError};
use crate::decoder::binlog_decoder::{BinlogReader, PAYLOAD_BUFFER_SIZE};
use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_header::{Header, HEADER_LEN};
use crate::events::log_context::{ILogContext, LogContext};
use crate::events::log_position::LogPosition;
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

/// Reads binlog events from a stream.
#[derive(Debug)]
pub struct FileBinlogReader {
    stream: File,
    is_symlink: bool,

    skip_magic_buffer: bool,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: Rc<RefCell<LogContext>>,

    /// stream 与 source_bytes 加载的缓冲区。 在一次BinlogReader中会被多次复用
    payload_buffer: Vec<u8>,

    eof: bool,
}

impl BinlogReader<File> for FileBinlogReader {
    fn new(context: Rc<RefCell<LogContext>>, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized {
        let none = File::create(Path::new("tmp")).unwrap();

        Ok(Self {
            stream: none,
            is_symlink: false,
            skip_magic_buffer,
            decoder: LogEventDecoder::new(),
            payload_buffer: vec![0; PAYLOAD_BUFFER_SIZE],
            context: context.clone(),
            eof: false,
        })
    }

    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, Rc<RefCell<LogContext>>), ReError> {
        let _context:LogContext = LogContext::new(LogPosition::new("test_demo"));
        let context = Rc::new(RefCell::new(_context));

        let rs = BinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        Ok((rs, context))
    }

    fn read_events(mut self, mut source: File) -> Self {
        if !self.skip_magic_buffer {
            // Parse magic
            let mut magic_buffer = vec![0; HEADER_LEN as usize];
            // read exactly HEADER_LEN bytes
            source.read_exact(&mut magic_buffer).unwrap();
            let (i, _) = Header::check_start(magic_buffer.as_slice()).unwrap();
            assert_eq!(i.len(), 0);
        }

        self.is_symlink = true;
        self.stream = source;

        self
    }

    fn get_context(&self) -> Rc<RefCell<LogContext>> {
        self.context.clone()
    }
}

impl FileBinlogReader {
    fn read_event(&mut self) -> Result<(Header, Event), ReError> {
        if !self.is_symlink {
            return Err(ReError::Incomplete(Needed::NoEnoughData));
        }

        let mut decoder = &mut self.decoder;

        // Parse header
        let mut header_buffer = [0; LOG_EVENT_HEADER_LEN as usize];
        self.stream.read_exact(&mut header_buffer)?;
        let header = Header::parse_v4_header(&header_buffer, self.context.clone()).unwrap();

        // parser payload
        let payload_length = header.event_length as usize - LOG_EVENT_HEADER_LEN as usize;

        if payload_length > PAYLOAD_BUFFER_SIZE {
            // 事件payload大小超过缓冲buffer，直接以事件payload大小分配新字节数组，用于读取事件的完整大小
            let mut vec: Vec<u8> = vec![0; payload_length];
            self.stream.read_exact(&mut vec)?;

            let (binlog_event, remain_bytes) = decoder.decode_with_slice(&vec, &header, self.context.clone()).unwrap();
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


/// Iterator
impl Iterator for FileBinlogReader {
    type Item = Result<(Header, Event), ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.read_event();
        self.context.borrow_mut().log_stat_add();

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

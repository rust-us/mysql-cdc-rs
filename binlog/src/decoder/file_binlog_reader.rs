use std::cell::RefCell;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::rc::Rc;
use common::err::decode_error::{Needed, ReError};
use crate::decoder::binlog_decoder::{BinlogReader, PAYLOAD_BUFFER_SIZE};
use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_header::{Header, HEADER_LEN};
use crate::events::event_raw::EventRaw;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::log_position::LogPosition;
use crate::events::protocol::format_description_log_event::LOG_EVENT_HEADER_LEN;

/// Reads binlog events from a stream.
#[derive(Debug)]
pub struct FileBinlogReader {
    stream: File,

    skip_magic_buffer: bool,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: LogContextRef,

    /// stream 与 source_bytes 加载的缓冲区。 在一次BinlogReader中会被多次复用
    payload_buffer: Vec<u8>,

    eof: bool,
}

impl BinlogReader<File, (Header, Event)> for FileBinlogReader {
    #[inline]
    fn new(context: LogContextRef, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized {
        let none = File::create(Path::new("tmp")).unwrap();

        Ok(Self {
            stream: none,
            // is_symlink: false,
            skip_magic_buffer,
            decoder: LogEventDecoder::new(),
            payload_buffer: vec![0; PAYLOAD_BUFFER_SIZE],
            context: context.clone(),
            eof: false,
        })
    }

    #[inline]
    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, LogContextRef), ReError> {
        let _context:LogContext = LogContext::new(LogPosition::new("test_demo"));
        let context = Rc::new(RefCell::new(_context));

        let rs = BinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        Ok((rs, context))
    }

    #[inline]
    fn read_events(&mut self, mut source: File) -> Box<dyn Iterator<Item=Result<(Header, Event), ReError>>> {
        if !self.skip_magic_buffer {
            // Parse magic
            let mut magic_buffer = vec![0; HEADER_LEN as usize];
            // read exactly HEADER_LEN bytes
            source.read_exact(&mut magic_buffer).unwrap();
            let (i, _) = Header::check_start(magic_buffer.as_slice()).unwrap();
            self.skip_magic_buffer = true;
            assert_eq!(i.len(), 0);
        }

        self.stream = source.try_clone().unwrap();

        Box::new(FileBinlogReaderIterator {
            index: 0,
            stream: source,
            skip_magic_buffer: self.skip_magic_buffer,
            decoder: self.decoder.clone(),
            context: self.context.clone(),
            payload_buffer: self.payload_buffer.clone(),
            eof: self.eof,
        })
    }

    #[inline]
    fn get_context(&self) -> LogContextRef {
        self.context.clone()
    }
}

struct FileBinlogReaderIterator {
    index: usize,

    stream: File,

    skip_magic_buffer: bool,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: LogContextRef,

    /// stream 与 source_bytes 加载的缓冲区。 在一次BinlogReader中会被多次复用
    payload_buffer: Vec<u8>,

    eof: bool,
}

impl FileBinlogReaderIterator {
    fn read_event(&mut self) -> Result<(Header, Event), ReError> {
        let mut decoder = &mut self.decoder;

        // Parse header
        let mut header_buffer = [0; LOG_EVENT_HEADER_LEN as usize];
        self.stream.read_exact(&mut header_buffer)?;
        let mut header = Header::parse_v4_header(&header_buffer, self.context.clone()).unwrap();
        let header_ref = Rc::new(RefCell::new(header.clone()));

        // parser payload
        let payload_length = header.event_length as usize - LOG_EVENT_HEADER_LEN as usize;

        if payload_length > PAYLOAD_BUFFER_SIZE {
            // 事件payload大小超过缓冲buffer，直接以事件payload大小分配新字节数组，用于读取事件的完整大小
            let mut vec: Vec<u8> = vec![0; payload_length];
            self.stream.read_exact(&mut vec)?;

            let binlog_event = decoder.decode_with_slice(&vec, header_ref, self.context.clone()).unwrap();

            Ok((header, binlog_event))
        } else {
            // 从缓冲区中取空字节数组，用于读取事件的完整大小
            let slice = &mut self.payload_buffer[0..payload_length];
            self.stream.read_exact(slice)?;

            let binlog_event = self.decoder.decode_with_slice(slice, header_ref, self.context.clone()).unwrap();

            Ok((header, binlog_event))
        }
    }
}


impl Iterator for FileBinlogReaderIterator {
    type Item = Result<(Header, Event), ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.read_event();

        return match result {
            Err(error) => {
                if let ReError::IoError(io_error) = &error {
                    // 文件读到了最后, is IoError(Error { kind: UnexpectedEof, message: "failed to fill whole buffer" })
                    if let ErrorKind::UnexpectedEof = io_error.kind() {
                        self.eof = true;
                        None
                    } else {
                        Some(Err(error))
                    }
                } else {
                    Some(Err(error))
                }
            },
            Ok(data) => {
                self.index += 1;
                self.context.borrow_mut().update_log_stat_add();

                Some(Ok(data))
            }
        }
    }
}
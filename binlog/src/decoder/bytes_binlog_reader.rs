use std::cell::RefCell;
use std::io::ErrorKind;
use std::rc::Rc;
use std::sync::Arc;
use std::vec::IntoIter;
use common::err::decode_error::ReError;
use crate::decoder::binlog_decoder::{BinlogReader};
use crate::decoder::event_decoder::{LogEventDecoder};
use crate::events::binlog_event::BinlogEvent;
use crate::events::event_raw::EventRaw;
use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::log_position::LogFilePosition;

/// Reads binlog events from a stream.
#[derive(Debug, Clone)]
pub struct BytesBinlogReader {
    /// 源内容。在读取结束后也可能会包含读取结束时的剩余字节。用于追加下一次请求中或者直接返回
    source_bytes: Vec<u8>,

    skip_magic_buffer: bool,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: LogContextRef,

    event_raw_iter: Arc<IntoIter<EventRaw>>,
}

impl BinlogReader<&[u8], BinlogEvent> for BytesBinlogReader {
    ///
    ///
    /// # Arguments
    ///
    /// * `stream`:
    /// * `skip_magic_buffer`:  是否跳过magic_number. true 表明已经跳过了（也就是说bytes中不存在magic_buffer）。 false指仍需执行 magic_number校验
    ///
    /// returns: Result<BytesBinlogReader, ReError>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    #[inline]
    fn new(context: LogContextRef, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized {
        let event_raw_list = Vec::new();

        Ok(Self {
            source_bytes: vec![],
            skip_magic_buffer,
            decoder: LogEventDecoder::new().unwrap(),
            context,
            event_raw_iter: Arc::new(event_raw_list.clone().into_iter().clone()),
        })
    }

    #[inline]
    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, LogContextRef), ReError> {
        let _context:LogContext = LogContext::new(LogFilePosition::new("BytesBinlogReader"));
        let context = Rc::new(RefCell::new(_context));

        let decoder = BinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        Ok((decoder, context))
    }

    #[inline]
    fn read_events(&mut self, stream: &[u8]) -> Box<dyn Iterator<Item=Result<BinlogEvent, ReError>>> {
        self.source_bytes = if !self.skip_magic_buffer {
            let (i, _) = Header::check_start(stream).unwrap();
            self.skip_magic_buffer = true;

            i.to_vec()
        } else {
            stream.to_vec()
        };

        let (remaining_bytes, event_raws) = EventRaw::steam_to_event_raw(&self.source_bytes, self.context.clone()).unwrap();
        self.source_bytes = remaining_bytes;
        self.event_raw_iter = Arc::new(event_raws.clone().into_iter());

        Box::new(BytesBinlogReaderIterator {
            index: 0,
            source_bytes: self.source_bytes.clone(),
            skip_magic_buffer: self.skip_magic_buffer,
            decoder: self.decoder.clone(),
            context: self.context.clone(),
            event_raws,
        })
    }

    #[inline]
    fn get_context(&self) -> LogContextRef {
        self.context.clone()
    }
}

impl BytesBinlogReader {
    #[inline]
    pub fn get_source_bytes(&self) -> Vec<u8> {
        self.source_bytes.clone()
    }
}


struct BytesBinlogReaderIterator {
    index: usize,

    /// 源内容。在读取结束后也可能会包含读取结束时的剩余字节。用于追加下一次请求中或者直接返回
    source_bytes: Vec<u8>,

    skip_magic_buffer: bool,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: LogContextRef,

    event_raws: Vec<EventRaw>,
}

impl Iterator for BytesBinlogReaderIterator {
    type Item = Result<BinlogEvent, ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.event_raws.len() {
            return None;
        }

        let event_raw = &self.event_raws[self.index];

        let header = event_raw.get_header();
        let full_packet = event_raw.get_payload();
        let result = self.decoder.event_parse(full_packet, header.clone(), self.context.clone());

        match result {
            Err(error) => {
                if let ReError::IoError(io_error) = &error {
                    if let ErrorKind::UnexpectedEof = io_error.kind() {
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
                self.context.borrow_mut().add_log_stat(data.len() as usize);

                Some(Ok(data))
            }
        }
    }
}
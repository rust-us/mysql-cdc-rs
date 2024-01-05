use std::cell::RefCell;
use std::io::ErrorKind;
use std::rc::Rc;
use std::sync::Arc;
use std::vec::IntoIter;
use common::err::DecodeError::ReError;
use crate::decoder::binlog_decoder::BinlogReader;
use crate::decoder::event_decoder::{EventDecoder, LogEventDecoder};
use crate::events::event::Event;
use crate::events::event_c::EventRaw;
use crate::events::event_factory::EventFactory;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::log_position::LogPosition;

/// Reads binlog events from a stream.
#[derive(Debug, Clone)]
pub struct BytesBinlogReader {
    /// 源内容。在读取结束后也可能会包含读取结束时的剩余字节。用于追加下一次请求中或者直接返回
    source_bytes: Vec<u8>,

    skip_magic_buffer: bool,

    /// stream 与 source_bytes 的解析器
    decoder: LogEventDecoder,

    context: Rc<RefCell<LogContext>>,

    event_raw_iter: Arc<IntoIter<EventRaw>>,

    eof: bool,
}

impl BinlogReader<&[u8]> for BytesBinlogReader {
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
    fn new(context: Rc<RefCell<LogContext>>, skip_magic_buffer: bool) -> Result<Self, ReError> where Self: Sized {
        let event_raw_list = Vec::new();

        Ok(Self {
            source_bytes: vec![],
            skip_magic_buffer,
            decoder: LogEventDecoder::new(),
            context,
            event_raw_iter: Arc::new(event_raw_list.clone().into_iter().clone()),
            eof: false,
        })
    }

    fn new_without_context(skip_magic_buffer: bool) -> Result<(Self, Rc<RefCell<LogContext>>), ReError> {
        let _context:LogContext = LogContext::new(LogPosition::new("BytesBinlogReader"));
        let context = Rc::new(RefCell::new(_context));

        let decoder = BinlogReader::new(context.clone(), skip_magic_buffer).unwrap();

        Ok((decoder, context))
    }

    fn read_events(mut self, stream: &[u8]) -> Self {
        self.source_bytes = if !self.skip_magic_buffer {
            let (i, _) = Header::check_start(stream).unwrap();
            i.to_vec()
        } else {
            stream.to_vec()
        };

        let (remaining_bytes, event_raws) = EventFactory::steam_to_event_raw(&self.source_bytes, self.context.clone()).unwrap();
        self.source_bytes = remaining_bytes.to_vec();
        self.event_raw_iter = Arc::new(event_raws.into_iter());

        self
    }

    fn get_context(&self) -> Rc<RefCell<LogContext>> {
        self.context.clone()
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
impl Iterator for BytesBinlogReader {
    type Item = Result<Event, ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        let it = Arc::get_mut(&mut self.event_raw_iter).unwrap();
        let event_raw = it.next();

        if event_raw.is_none() {
            return None;
        }

        let result = self.read_event(&event_raw.unwrap());
        self.context.borrow_mut().log_stat_add();

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

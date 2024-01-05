use std::cell::RefCell;
use std::io::{Cursor, Read};
use std::rc::Rc;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::events::checksum_type::ST_COMMON_PAYLOAD_CHECKSUM_LEN;
use crate::events::event_header::Header;
use crate::events::log_context::LogContext;
use crate::events::log_event::LogEvent;
use crate::events::protocol::format_description_log_event::{LOG_EVENT_HEADER_LEN};
use crate::utils::read_variable_len_string;

/// 最后一个rotate event用于说明下一个binlog文件。
/// Last event in a binlog file which points to next binlog file.
/// Fake version is also returned when replication is started.
/// <a href="https://mariadb.com/kb/en/library/rotate_event/">See more</a>
#[derive(Debug, Serialize, Clone)]
pub struct RotateEvent {
    header: Header,

    /// Gets next binlog filename
    binlog_filename: String,

    /// Gets next binlog position
    binlog_position: u64,
}

impl RotateEvent {
    pub fn get_file_name(&self) -> String {
        self.binlog_filename.clone()
    }

    pub fn get_binlog_position(&self) -> u64 {
        self.binlog_position.clone()
    }

    pub fn new(header: Header, binlog_filename: String, binlog_position: u64) -> Self {
        RotateEvent {
            header,
            binlog_filename,
            binlog_position,
        }
    }

    pub fn parse(
        cursor: &mut Cursor<&[u8]>, header: &Header, context: Rc<RefCell<LogContext>>) -> Result<RotateEvent, ReError> {
        let _context = context.borrow();
        let post_header_len = _context.get_format_description().get_post_header_len(header.get_event_type() as usize);

        let position = cursor.read_u64::<LittleEndian>()?;

        let binlog_filename_len = header.event_length -
                    (LOG_EVENT_HEADER_LEN + post_header_len + ST_COMMON_PAYLOAD_CHECKSUM_LEN) as u32;
        let mut _rows_data_vec = vec![0; binlog_filename_len as usize];
        cursor.read_exact(&mut _rows_data_vec)?;
        let next_binlog_filename = read_variable_len_string(&_rows_data_vec, binlog_filename_len as usize);
        // let mut next_binlog_filename = String::new();
        // cursor.read_to_string(&mut next_binlog_filename)?;

        let checksum = cursor.read_u32::<LittleEndian>()?;

        Ok(
            RotateEvent::new(
                Header::copy_and_get(header, checksum, vec![]),
                next_binlog_filename, position
            )
        )
    }
}

impl LogEvent for RotateEvent {

}
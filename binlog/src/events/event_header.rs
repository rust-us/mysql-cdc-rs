use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use nom::{
    bytes::complete::{tag},
    IResult,
};
use serde::Serialize;
use common::err::DecodeError::ReError;
use crate::b_type::LogEventType::{FORMAT_DESCRIPTION_EVENT, ROTATE_EVENT};
use crate::events::event_header_flag::EventFlag;
use crate::events::event_raw::HeaderRef;
use crate::events::gtid_set::MysqlGTIDSet;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::protocol::gtid_log_event::GtidLogEvent;

pub const HEADER_LEN: u8 = 4;

pub const GTID_SET_STRING: &str = "gtid_str";
pub const CURRENT_GTID_STRING: &str = "curt_gtid";
pub const CURRENT_GTID_SN: &str= "curt_gtid_sn";
pub const CURRENT_GTID_LAST_COMMIT: &str = "curt_gtid_lct";

/////////////////////////////////////
///  EventHeader Header
///
/// 从mysql5.0版本开始，binlog采用的是v4版本，第一个event都是format_desc event 用于描述binlog文件的格式版本，
/// 每个event都有一个19个字节的Binlog Event Header
///
/// binlog 采用小端序列，也就说 server-id 为1 的u32 应该为 0x01 0x00 0x00 0x00。
///
///                      [startPos : Len]
/// +=====================================+
/// | event  | timestamp         0 : 4    |  ==> 前面4个字节是固定的magic number,值为0x6e6962fe。
/// | header +----------------------------+
/// |        | event_type        4 : 1    | = FORMAT_DESCRIPTION_EVENT = 15
/// |        +----------------------------+
/// |        | server_id         5 : 4    |
/// |        +----------------------------+
/// |        | event_length      9 : 4    | >= 91
/// |        +----------------------------+
/// |        | next_position    13 : 4    |
/// |        +----------------------------+
/// |        | flags            17 : 2    |
/// +=====================================+
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Header {
    /// 4字节的 timestamp, Provides creation time in seconds from Unix.
    pub when: u32,

    /// 1个代表事件类型的 u8,  Gets type of the binlog event.
    pub event_type: u8,

    /// u32 的 server_id。 4 byte， 该id表明binlog的源server是哪个，用来在循环复制 binlog event）
    ///
    /// The master's server id (is preserved in the relay log;
    /// used to prevent from infinite loops in circular replication).
    pub server_id: u32,

    /// u32 事件大小， Gets event length (header + event + checksum).
    /// Number of bytes written by write() function
    pub event_length: u32,

    /// Gets file position of next event.
    ///
    /// The offset in the log where this event originally appeared (it is
    /// preserved in relay logs, making SHOW SLAVE STATUS able to print coordinates of the event in the master's binlog).
    /// Note: when a transaction is written by the master to its binlog (wrapped in
    /// BEGIN/COMMIT) the log_pos of all the queries it contains is the one of
    /// the BEGIN (this way, when one does SHOW SLAVE STATUS it sees the offset of the BEGIN,
    /// which is logical as rollback may occur), except the COMMIT query which has its real offset.
    ///
    pub log_pos: u32,

    /// Gets event flags.
    /// Some 16 flags. See the definitions above for LOG_EVENT_TIME_F, LOG_EVENT_FORCED_ROTATE_F,
    /// LOG_EVENT_THREAD_SPECIFIC_F, and LOG_EVENT_SUPPRESS_USE_F for notes.
    ///
    /// See <a href="https://mariadb.com/kb/en/2-binlog-event-header/#event-flag">documentation</a>.
    pub flags: u16,
    pub flags_attr: EventFlag,

    ///////////////////////////////////////////////////
    // other
    ///////////////////////////////////////////////////
    /// The value is set by caller of FD constructor and Log_event::write_header() for the rest.
    /// In the FD case it's propagated into the last byte of post_header_len[] at FD::write().
    /// On the slave side the value is assigned from post_header_len[last] of the last seen FD event.
    pub checksum_alg: u8,

    /// checksum, Placeholder for event checksum while writing to binlog.
    pub checksum: u32,

    log_file_name : String,

    pub gtid_map : HashMap<String, String>,
}

impl Default for Header {
    fn default() -> Self {
        Header {
            when: 0,
            event_type: 0,
            server_id: 0,
            event_length: 0,
            log_pos: 0,
            flags: 0,
            flags_attr: Default::default(),
            checksum_alg: 0,
            checksum: 0,
            log_file_name: "".to_string(),
            gtid_map: HashMap::new(),
        }
    }
}

impl Header {

    pub fn get_event_length(&self) -> u32 {
        self.event_length
    }

    pub fn get_event_type(&self) -> u8 {
        self.event_type
    }

    pub fn get_log_file_name(&self) -> String {
        self.log_file_name.clone()
    }

    pub fn get_flags_attr(&self) -> EventFlag {
        EventFlag::from(self.flags)
    }

    pub fn get_log_pos(&self) -> u32 {
        self.log_pos.clone()
    }

    pub fn set_checksum(&mut self, checksum: u32) {
        self.checksum = checksum;
    }

    pub fn update_gtid(&mut self, gtid_set: Option<&MysqlGTIDSet>, gtid_event: Option<&GtidLogEvent>) {
        if gtid_set.is_none() {
            return;
        }

        let gtid = gtid_set.unwrap();
        self.gtid_map.insert(GTID_SET_STRING.to_string(), gtid.to_string());

        if gtid_event.is_none() {
            return;
        }

        let e = gtid_event.unwrap();
        self.gtid_map.insert(CURRENT_GTID_STRING.to_string(), e.get_gtid_str());
        self.gtid_map.insert(CURRENT_GTID_SN.to_string(), e.get_sequence_number().to_string());
        self.gtid_map.insert(CURRENT_GTID_LAST_COMMIT.to_string(), e.get_last_committed().to_string());
    }

    pub fn update_checksum(&mut self, checksum: u32) {
        self.checksum = checksum;
    }

    pub fn new(log_file_name:String, when: u32,
               event_type: u8, server_id: u32,
               event_length: u32, log_pos: u32,
               flags: u16) -> Self {
        let flags_attr = EventFlag::from(flags);

        Header::new_with_checksum_alg(log_file_name, when, event_type, server_id,
                                      event_length, log_pos, flags, 0)
    }

    pub fn new_with_checksum_alg(log_file_name:String, when: u32,
               event_type: u8, server_id: u32,
               event_length: u32, log_pos: u32,
               flags: u16, checksum_alg: u8) -> Self {
        let flags_attr = EventFlag::from(flags);

        Header {
            when,
            event_type,
            server_id,
            event_length,
            log_pos,
            flags,
            flags_attr,
            checksum_alg,
            checksum: 0,
            log_file_name,
            gtid_map: HashMap::new(),
        }
    }

    /// binlog文件以一个值为0Xfe62696e的魔数开头，这个魔数对应0xfe 'b''i''n'。
    pub fn check_start(i: &[u8]) -> IResult<&[u8], &[u8]> {
        tag([254, 98, 105, 110])(i)
    }

    /// 解析 header
    pub fn parse_v4_header(bytes: &[u8], context: LogContextRef) -> Result<Header, ReError> {
        let mut cursor = Cursor::new(bytes);
        let mut checksum_alg = 0;

        let timestamp = cursor.read_u32::<LittleEndian>()?;
        let event_type = cursor.read_u8()?;
        let server_id = cursor.read_u32::<LittleEndian>()?;
        let event_length = cursor.read_u32::<LittleEndian>()?;

        let _context = context.borrow();
        let current_binlog_version = _context.get_format_description().binlog_version;

        if current_binlog_version == 1 {
            return
                Ok(
                    Header::new_with_checksum_alg("".to_string(), timestamp, event_type, server_id,
                                                  event_length, 0, 0, checksum_alg),
                );
        }

        // 4.0 or newer
        let mut log_pos = cursor.read_u32::<LittleEndian>()?;

        // If the log is 4.0 (so here it can only be a 4.0 relay log read by the SQL thread or a 4.0 master binlog read by the I/O thread),
        // log_pos is the beginning of the event:
        // we transform it into the end of the event, which is more useful.
        //
        // But how do you know that the log is 4.0:
        // you know it if description_event is version 3 *and* you are not reading a Format_desc (remember that mysqlbinlog starts by assuming that 5.0 logs are in 4.0 format,
        // until it finds a Format_desc).
        if current_binlog_version == 3
            && event_type < FORMAT_DESCRIPTION_EVENT as u8
            && log_pos != 0 {
            // If log_pos=0, don't change it. log_pos==0 is a marker to mean
            // "don't change rli->group_master_log_pos" (see inc_group_relay_log_pos()).
            // As it is unreal log_pos, adding the event len's is nonsense.
            //
            // For example, a fake Rotate event should not have its log_pos (which is 0) changed or it will modify
            // Exec_master_log_pos in SHOW SLAVE STATUS, displaying a nonsense
            // value of (a non-zero offset which does not exist in the master's binlog,
            // so which will cause problems if the user uses this value in CHANGE MASTER).
            log_pos += event_length;
        }

        let flags = cursor.read_u16::<LittleEndian>()?;

        if event_type == ROTATE_EVENT as u8 {
            return
                Ok(
                    Header::new_with_checksum_alg("".to_string(), timestamp, event_type, server_id,
                                                  event_length, log_pos, flags, checksum_alg),
                );
        }

        if event_type == FORMAT_DESCRIPTION_EVENT as u8 {
            // These events always have a header which stops here (i.e. their header is FROZEN).
            //
            // Initialization to zero of all other Log_event members as they're not specified.
            // Currently there are no such members;
            // in the future there will be an event UID (but Format_description and Rotate don't need this UID,
            // as they are not propagated through --log-slave-updates (remember the UID is used to not play a query
            // twice when you have two masters which are slaves of a 3rd master).
            // Then we are done.

            // need do parser checksumAlg, parser crc
            return
                Ok(
                    Header::new_with_checksum_alg("".to_string(), timestamp, event_type, server_id,
                                                  event_length, log_pos, flags, checksum_alg),
                );
        }

        // need do parser checksumAlg, parser crc
        Ok(
            Header::new_with_checksum_alg("".to_string(), timestamp, event_type, server_id,
                                          event_length, log_pos, flags, checksum_alg),
        )
    }

    pub fn copy(source: HeaderRef) -> Self  {
        let log_file_name : String = source.borrow().get_log_file_name();

        Header {
            when: source.borrow().when,
            event_type: source.borrow().event_type,
            server_id: source.borrow().server_id,
            event_length: source.borrow().event_length,
            log_pos: source.borrow().log_pos,
            flags: source.borrow().flags,
            flags_attr: source.borrow().get_flags_attr(),
            checksum_alg: source.borrow().checksum_alg,
            checksum: source.borrow().checksum,
            log_file_name,
            gtid_map: source.borrow().gtid_map.clone(),
        }
    }

    pub fn copy_and_get(source: HeaderRef, checksum: u32, gtid_map : HashMap<String, String>) -> Self  {
        let log_file_name : String = source.borrow().get_log_file_name();

        Header {
            when: source.borrow().when,
            event_type: source.borrow().event_type,
            server_id: source.borrow().server_id,
            event_length: source.borrow().event_length,
            log_pos: source.borrow().log_pos,
            flags: source.borrow().flags,
            flags_attr: source.borrow().get_flags_attr(),
            checksum_alg: source.borrow().checksum_alg,
            checksum,
            log_file_name,
            gtid_map,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::events::log_context::LogContext;
    use crate::events::log_position::LogPosition;

    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}

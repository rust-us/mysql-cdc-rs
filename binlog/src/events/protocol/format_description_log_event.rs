use crate::events::event_header::Header;
use crate::events::protocol::start_log_event_v3::*;

use nom::{
    bytes::complete::{take},
    combinator::map,
    number::complete::{le_u16, le_u32, le_u8},
    IResult,
};
use serde::Serialize;
use crate::b_type::{C_ENUM_END_EVENT, LogEventType};
use crate::b_type::LogEventType::*;
use crate::events::checksum_type::{BINLOG_CHECKSUM_ALG_DESC_LEN, ChecksumType, ST_COMMON_PAYLOAD_CHECKSUM_LEN};
use crate::events::event::Event::*;
use crate::events::log_event::*;
use crate::utils::extract_string;

//
// public static final int

/// The number of types we handle in Format_description_log_event
/// (UNKNOWN_EVENT is not to be handled, it does not exist in binlogs, it
/// does not have a format).
pub const LOG_EVENT_TYPES: usize = (C_ENUM_END_EVENT - 1) as usize;

/// 除了checksum_alg、checksum 之外的 payload 大小： (2 + 50 + 4 + 1)
pub const ST_COMMON_PAYLOAD_WITHOUT_CHECKSUM_LEN: u8 = (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN + 4 + 1);
pub const ST_COMMON_HEADER_LEN_OFFSET: u8 =  ST_COMMON_PAYLOAD_WITHOUT_CHECKSUM_LEN;
// pub const ST_COMMON_HEADER_LEN_OFFSET: isize =  (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN + 4);

/// header 大小
pub const OLD_HEADER_LEN: u8 = 13;
pub const LOG_EVENT_HEADER_LEN: u8 = 19;
pub const LOG_EVENT_MINIMAL_HEADER_LEN: u8 = 19;


///////////////////////////////////////////
/// event-specific post-header sizes
///////////////////////////////////////////
pub const STOP_HEADER_LEN: u8 = 0;
pub const LOAD_HEADER_LEN: u8 = (4 + 4 + 4 + 1 + 1 + 4);
pub const SLAVE_HEADER_LEN: u8 = 0;
pub const START_V3_HEADER_LEN: u8 = (2 + ST_SERVER_VER_LEN + 4);
pub const ROTATE_HEADER_LEN: u8 = 8;
pub const INTVAR_HEADER_LEN: u8 = 0;
pub const CREATE_FILE_HEADER_LEN: u8 = 4;
pub const APPEND_BLOCK_HEADER_LEN: u8 = 4;
pub const EXEC_LOAD_HEADER_LEN: u8 = 4;
pub const DELETE_FILE_HEADER_LEN: u8 = 4;
pub const NEW_LOAD_HEADER_LEN: u8 = LOAD_HEADER_LEN;
pub const RAND_HEADER_LEN: u8 = 0;
pub const USER_VAR_HEADER_LEN: u8 = 0;
pub const FORMAT_DESCRIPTION_HEADER_LEN: u8 = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES as u8);
pub const XID_HEADER_LEN: u8 = 0;
pub const BEGIN_LOAD_QUERY_HEADER_LEN: u8 = APPEND_BLOCK_HEADER_LEN;

pub const ROWS_HEADER_LEN_V1: u8 = 8;
pub const TABLE_MAP_HEADER_LEN: u8 = 8;
pub const EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN: u8 = (4 + 4 + 4 + 1);
pub const EXECUTE_LOAD_QUERY_HEADER_LEN: u8 = (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN);
pub const INCIDENT_HEADER_LEN: u8 = 2;
pub const HEARTBEAT_HEADER_LEN: u8 = 0;
pub const IGNORABLE_HEADER_LEN: u8 = 0;
pub const ROWS_HEADER_LEN_V2: u8 = 10;
pub const TRANSACTION_CONTEXT_HEADER_LEN: u8 = 18;
pub const VIEW_CHANGE_HEADER_LEN: u8 = 52;
pub const XA_PREPARE_HEADER_LEN: u8 = 0;
pub const TRANSACTION_PAYLOAD_HEADER_LEN: u8 = 0;

pub const ANNOTATE_ROWS_HEADER_LEN: u8 = 0;
pub const BINLOG_CHECKPOINT_HEADER_LEN: u8 = 4;
pub const GTID_HEADER_LEN: u8 = 19;
pub const GTID_LIST_HEADER_LEN: u8 = 4;
pub const START_ENCRYPTION_HEADER_LEN: u8 = 0;
pub const POST_HEADER_LENGTH: u8 = 11;


/// source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L295-L344
/// event_data layout: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L387-L416
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct FormatDescriptionEvent {
    header: Header,

    /// binlog版本, 大于1时 header 是19个字节。 等于1时header 13个字节。binlog的版本，一般有1，3，4
    pub binlog_version: u16,

    /// MySql的版本
    pub server_version: String,

    /// Gets checksum algorithm type.
    checksum_type: ChecksumType,

    /// binlog创建的时间 binlog文件是可追加的，这里应该理解成binlog的追加时间
    pub create_timestamp: u32,

    /// The size of the fixed header which _all_ events have (for binlogs written
    /// by this version, this is equal to LOG_EVENT_HEADER_LEN),
    /// except FORMAT_DESCRIPTION_EVENT and ROTATE_EVENT (those have a header of size  LOG_EVENT_MINIMAL_HEADER_LEN).
    ///
    /// 之后所有event的公共头长度，一般是19
    pub common_header_len: u8,

    /// 一个数组表示Binlog Event Type 主要是列出所有事件的Post-Header的大小，每个字节表示一种事件类型。
    /// The list of post-headers' lengthes
    post_header_len: Vec<u8>,

    number_of_event_types: u32,

    declare: FormatDescriptionDeclare,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct FormatDescriptionDeclare {
    pub fdv: FormatDescriptionsVersion,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum FormatDescriptionsVersion {
    /// MySQL 3.23 format descriptions
    V3_23,

    /// MySQL 4.0.x (x>=2) format descriptions
    V4_0_x,

    /// MySQL 5.0 format descriptions
    V5_0,

}

impl FormatDescriptionDeclare {
    pub fn new(binlog_version: u16) -> Self {
        match binlog_version {
            4 => {
                FormatDescriptionDeclare {
                    fdv: FormatDescriptionsVersion::V5_0,
                }
            },
            3 => {
                FormatDescriptionDeclare {
                    fdv: FormatDescriptionsVersion::V4_0_x,
                }
            },
            1 => {
                FormatDescriptionDeclare {
                    fdv: FormatDescriptionsVersion::V3_23,
                }
            },
            _ => {
                log::error!("unexpected binlog_version: {:x}", binlog_version);
                unreachable!();
            }
        }
    }
}

impl Default for FormatDescriptionEvent {
    fn default() -> Self {
        FormatDescriptionEvent::declare(4)
    }
}

impl FormatDescriptionEvent {
    pub fn is_v1(&self) -> bool {
        self.declare.fdv == FormatDescriptionsVersion::V3_23
        // self.binlog_version == 1
    }

    pub fn is_v3(&self) -> bool {
        self.declare.fdv == FormatDescriptionsVersion::V4_0_x
        // self.binlog_version == 3

    }

    pub fn is_v4(&self) -> bool {
        self.declare.fdv == FormatDescriptionsVersion::V5_0
        // self.binlog_version == 4
    }

    pub fn get_checksum_type(&self) -> ChecksumType {
        self.checksum_type.clone()
    }

    /// 得到该事件类型的 Post-Header 部分的长度
    ///
    /// # Arguments
    ///
    /// * `b_type`: 事件类型
    ///
    /// returns: u8, 该事件类型的 Post-Header 部分的长度
    ///
    /// # Examples
    ///
    /// ```
    ///   let query_post_header_len = context.borrow().get_format_description().get_post_header_len(header.get_event_type() as usize);
    ///   query_post_header_len
    /// ```
    pub fn get_post_header_len(&self, b_type: usize) -> u8 {
        self.post_header_len[b_type - 1]
    }

    pub fn get_declare(self) -> FormatDescriptionDeclare {
        self.declare.clone()
    }

    pub fn declare(binlog_version: u16) -> Self {
        let mut server_version:String = String::default();
        let mut common_header_len:u8 = LOG_EVENT_HEADER_LEN;
        let mut number_of_event_types:u32 = LOG_EVENT_TYPES as u32;
        let mut post_header_len = vec![0;LogEventType::ENUM_END_EVENT.as_val()];

        let mut create_timestamp:u32 = 0;

        match binlog_version {
            4 => {
                // MySQL 5.0
                server_version = SERVER_VERSION.to_string();
                common_header_len = LOG_EVENT_HEADER_LEN;
                number_of_event_types = LOG_EVENT_TYPES as u32;

                // Note: all event types must explicitly fill in their lengths here.
                post_header_len[START_EVENT_V3.as_val() - 1] = START_V3_HEADER_LEN;
                post_header_len[QUERY_EVENT.as_val() - 1] = QUERY_HEADER_LEN;
                post_header_len[STOP_EVENT.as_val() - 1] = STOP_HEADER_LEN;
                post_header_len[ROTATE_EVENT.as_val() - 1] = ROTATE_HEADER_LEN;
                post_header_len[INTVAR_EVENT.as_val() - 1] = INTVAR_HEADER_LEN;
                post_header_len[LOAD_EVENT.as_val() - 1] = LOAD_HEADER_LEN;
                post_header_len[SLAVE_EVENT.as_val() - 1] = SLAVE_HEADER_LEN;
                post_header_len[CREATE_FILE_EVENT.as_val() - 1] = CREATE_FILE_HEADER_LEN;
                post_header_len[APPEND_BLOCK_EVENT.as_val() - 1] = APPEND_BLOCK_HEADER_LEN;
                post_header_len[EXEC_LOAD_EVENT.as_val() - 1] = EXEC_LOAD_HEADER_LEN;
                post_header_len[DELETE_FILE_EVENT.as_val() - 1] = DELETE_FILE_HEADER_LEN;
                post_header_len[NEW_LOAD_EVENT.as_val() - 1] = NEW_LOAD_HEADER_LEN;
                post_header_len[RAND_EVENT.as_val() - 1] = RAND_HEADER_LEN;
                post_header_len[USER_VAR_EVENT.as_val() - 1] = USER_VAR_HEADER_LEN;
                post_header_len[FORMAT_DESCRIPTION_EVENT.as_val() - 1] = FORMAT_DESCRIPTION_HEADER_LEN;
                post_header_len[XID_EVENT.as_val() - 1] = XID_HEADER_LEN;
                post_header_len[BEGIN_LOAD_QUERY_EVENT.as_val() - 1] = BEGIN_LOAD_QUERY_HEADER_LEN;
                post_header_len[EXECUTE_LOAD_QUERY_EVENT.as_val() - 1] = EXECUTE_LOAD_QUERY_HEADER_LEN;
                post_header_len[TABLE_MAP_EVENT.as_val() - 1] = TABLE_MAP_HEADER_LEN;
                post_header_len[WRITE_ROWS_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                post_header_len[UPDATE_ROWS_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                post_header_len[DELETE_ROWS_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                /*
                 * We here have the possibility to simulate a master of before
                 * we changed the table map id to be stored in 6 bytes: when it
                 * was stored in 4 bytes (=> post_header_len was 6). This is
                 * used to test backward compatibility. This code can be removed
                 * after a few months (today is Dec 21st 2005), when we know
                 * that the 4-byte masters are not deployed anymore (check with
                 * Tomas Ulin first!), and the accompanying test
                 * (rpl_row_4_bytes) too.
                 */
                post_header_len[HEARTBEAT_LOG_EVENT.as_val() - 1] = 0;
                post_header_len[IGNORABLE_LOG_EVENT.as_val() - 1] = IGNORABLE_HEADER_LEN;
                post_header_len[ROWS_QUERY_LOG_EVENT.as_val() - 1] = IGNORABLE_HEADER_LEN;
                post_header_len[WRITE_ROWS_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[UPDATE_ROWS_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[DELETE_ROWS_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[GTID_LOG_EVENT.as_val() - 1] = POST_HEADER_LENGTH;
                post_header_len[ANONYMOUS_GTID_LOG_EVENT.as_val() - 1] = POST_HEADER_LENGTH;
                post_header_len[PREVIOUS_GTIDS_LOG_EVENT.as_val() - 1] = IGNORABLE_HEADER_LEN;

                post_header_len[TRANSACTION_CONTEXT_EVENT.as_val() - 1] = TRANSACTION_CONTEXT_HEADER_LEN;
                post_header_len[VIEW_CHANGE_EVENT.as_val() - 1] = VIEW_CHANGE_HEADER_LEN;
                post_header_len[XA_PREPARE_LOG_EVENT.as_val() - 1] = XA_PREPARE_HEADER_LEN;
                post_header_len[PARTIAL_UPDATE_ROWS_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[TRANSACTION_PAYLOAD_EVENT.as_val() - 1] = TRANSACTION_PAYLOAD_HEADER_LEN;

                // mariadb 10
                post_header_len[ANNOTATE_ROWS_EVENT.as_val() - 1] = ANNOTATE_ROWS_HEADER_LEN;
                post_header_len[BINLOG_CHECKPOINT_EVENT.as_val() - 1] = BINLOG_CHECKPOINT_HEADER_LEN;
                post_header_len[GTID_EVENT.as_val() - 1] = GTID_HEADER_LEN;
                post_header_len[GTID_LIST_EVENT.as_val() - 1] = GTID_LIST_HEADER_LEN;
                post_header_len[START_ENCRYPTION_EVENT.as_val() - 1] = START_ENCRYPTION_HEADER_LEN;

                // mariadb compress
                post_header_len[QUERY_COMPRESSED_EVENT.as_val() - 1] = QUERY_COMPRESSED_EVENT as u8;
                post_header_len[WRITE_ROWS_COMPRESSED_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[UPDATE_DELETE_ROWS_COMPRESSED_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[UPDATE_DELETE_ROWS_COMPRESSED_EVENT.as_val()] = ROWS_HEADER_LEN_V2;
                post_header_len[WRITE_ROWS_COMPRESSED_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                post_header_len[UPDATE_ROWS_COMPRESSED_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                post_header_len[DELETE_ROWS_COMPRESSED_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
            },
            3 => {
                //  4.0.x x>=2
                server_version = SERVER_VERSION_4.to_string();
                common_header_len = LOG_EVENT_MINIMAL_HEADER_LEN as u8;

                /*
                 * The first new event in binlog version 4 is Format_desc. So
                 * any event type after that does not exist in older versions.
                 * We use the events known by version 3, even if version 1 had
                 * only a subset of them (this is not a problem: it uses a few
                 * bytes for nothing but unifies code; it does not make the
                 * slave detect less corruptions).
                 */
                number_of_event_types = (FORMAT_DESCRIPTION_EVENT.as_val() - 1) as u32;

                // Note: all event types must explicitly fill in their lengths here.
                post_header_len[START_EVENT_V3.as_val() - 1] = START_V3_HEADER_LEN;
                post_header_len[QUERY_EVENT.as_val() - 1] = QUERY_HEADER_MINIMAL_LEN;
                post_header_len[ROTATE_EVENT.as_val() - 1] = ROTATE_HEADER_LEN;
                post_header_len[LOAD_EVENT.as_val() - 1] = LOAD_HEADER_LEN;
                post_header_len[CREATE_FILE_EVENT.as_val() - 1] = CREATE_FILE_HEADER_LEN;
                post_header_len[APPEND_BLOCK_EVENT.as_val() - 1] = APPEND_BLOCK_HEADER_LEN;
                post_header_len[EXEC_LOAD_EVENT.as_val() - 1] = EXEC_LOAD_HEADER_LEN;
                post_header_len[DELETE_FILE_EVENT.as_val() - 1] = DELETE_FILE_HEADER_LEN;
                post_header_len[NEW_LOAD_EVENT.as_val() - 1] = post_header_len[LOAD_EVENT.as_val() - 1];
            },
            1 => {
                // 3.23
                server_version = SERVER_VERSION_3.to_string();
                common_header_len = OLD_HEADER_LEN as u8;

                /*
                * The first new event in binlog version 4 is Format_desc. So
                * any event type after that does not exist in older versions.
                * We use the events known by version 3, even if version 1 had
                * only a subset of them (this is not a problem: it uses a few
                * bytes for nothing but unifies code; it does not make the
                * slave detect less corruptions).
                */
                number_of_event_types = (FORMAT_DESCRIPTION_EVENT.as_val() - 1) as u32;

                post_header_len[START_EVENT_V3.as_val() - 1] = START_V3_HEADER_LEN;
                post_header_len[QUERY_EVENT.as_val() - 1] = QUERY_HEADER_MINIMAL_LEN;
                post_header_len[LOAD_EVENT.as_val() - 1] = LOAD_HEADER_LEN;
                post_header_len[CREATE_FILE_EVENT.as_val() - 1] = CREATE_FILE_HEADER_LEN;
                post_header_len[APPEND_BLOCK_EVENT.as_val() - 1] = APPEND_BLOCK_HEADER_LEN;
                post_header_len[EXEC_LOAD_EVENT.as_val() - 1] = EXEC_LOAD_HEADER_LEN;
                post_header_len[DELETE_FILE_EVENT.as_val() - 1] = DELETE_FILE_HEADER_LEN;
                post_header_len[NEW_LOAD_EVENT.as_val() - 1] = post_header_len[LOAD_EVENT.as_val() - 1];
            },
            _ => {
                common_header_len = 0;
                number_of_event_types = 0;
            }
        }

        FormatDescriptionEvent {
            header: Header::default(),
            binlog_version,
            server_version,
            checksum_type: ChecksumType::None,
            create_timestamp,
            common_header_len,
            post_header_len,
            number_of_event_types,
            declare: FormatDescriptionDeclare::new(binlog_version),
        }

    }

    /// format_desc event格式   [startPos : Len]
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
    /// +=====================================+                         payload 结构
    /// | payload| binlog_version   19 : 2    | = 4             2                binlog-version
    /// |        +----------------------------+                 string[50]       mysql-server version
    /// |        | server_version   21 : 50   |                 4                create timestamp
    /// |        +----------------------------+                 1                event header length
    /// |        | create_timestamp 71 : 4    |                 string[p]        event type header lengths
    /// |        +----------------------------+                 1                checksum alg
    /// |        | header_length    75 : 1    |                 4                checksum
    /// |        +----------------------------+
    /// |        | post-header      76 : n    | = array of n bytes, one byte per event
    /// |        | lengths for all            |   type that the server knows about
    /// |        | event types                |
    /// |        +----------------------------+
    /// |        | checksum alg   76+n : 1    |
    /// |        +----------------------------+
    /// |        | checksum      76+n+5 : 4   |
    /// |        +----------------------------+
    /// +=====================================+
    pub fn parse<'a>(input: &'a [u8], header: &Header) -> IResult<&'a [u8], FormatDescriptionEvent> {
        // let declare: FormatDescriptionDeclare = context.get_format_description().get_declare();
        let declare: FormatDescriptionDeclare = FormatDescriptionDeclare::new(4);

        // 一个 u16 代表的 binlog 版本
        let (i, binlog_version) = le_u16(input)?;
        // 一个固定长度为 50 的字符串（可能包含多个 \0 终止符)
        let (i, server_version) = map(take(ST_SERVER_VER_LEN), |s: &[u8]| extract_string(s))(i)?;
        /* Redundant timestamp & header length which is always 19 */
        // u32 的 timestamp
        let (i, create_timestamp) = le_u32(i)?;
        // let create_timestamp = 0;
        let (i, common_header_len) = le_u8(i)?;

        // 一直到事件结尾(去除后面 checksum 和算法)的数组
        // supported_types 要取多少个字节 = header.event_size - 19[header 大小] - (2 + 50 + 4 + 1) - 1[checksum_alg size] - 4[checksum size]
        // 剩下的就是 supported_types 占用的字节数
        let number_of_event_types = header.event_length -
            (LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_PAYLOAD_WITHOUT_CHECKSUM_LEN) as u32
            - BINLOG_CHECKSUM_ALG_DESC_LEN as u32 - ST_COMMON_PAYLOAD_CHECKSUM_LEN as u32;

        let (i, post_header_len) = map(take(number_of_event_types as u8), |s: &[u8]| s.to_vec())(i)?;

        let mut checksum_type = ChecksumType::None;
        let (i, checksum_alg) = le_u8(i)?;
        checksum_type = ChecksumType::from_code(checksum_alg).unwrap();

        let (i, crc) = le_u32(i)?;

        let header_new:Header = Header::copy_and_get(&header, crc, Vec::new());

        Ok((
            i,
            FormatDescriptionEvent {
                header: header_new,
                binlog_version,
                server_version,
                checksum_type,
                create_timestamp,
                common_header_len,
                post_header_len: post_header_len,
                number_of_event_types,
                declare
            },
        ))
    }
}

impl LogEvent for FormatDescriptionEvent {

}
use std::collections::HashMap;
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use lazy_static::lazy_static;
use crate::events::event_header::Header;

use crate::b_type::LogEventType::*;
use crate::b_type::{LogEventType, C_ENUM_END_EVENT};
use crate::events::checksum_type::{ChecksumType, BINLOG_CHECKSUM_ALG_DESC_LEN, ST_COMMON_PAYLOAD_CHECKSUM_LEN, BINLOG_CHECKSUM_ALG_UNDEF};
use crate::events::binlog_event::BinlogEvent::*;
use crate::events::declare::log_event::*;
use crate::utils::extract_string;
use serde::Serialize;
use tracing::error;
use common::err::decode_error::ReError;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::protocol::v4::start_v3_event::*;

/// The number of types we handle in Format_description_log_event
/// (UNKNOWN_EVENT is not to be handled, it does not exist in binlogs, it
/// does not have a format).
pub const LOG_EVENT_TYPES: usize = (C_ENUM_END_EVENT - 1) as usize;

/// 除了checksum_alg、checksum 之外的 payload 大小： (2 + 50 + 4)
pub const ST_COMMON_PAYLOAD_WITHOUT_CHECKSUM_LEN: u8 =
    (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN + 4);
pub const ST_COMMON_HEADER_LEN_OFFSET: u8 = ST_COMMON_PAYLOAD_WITHOUT_CHECKSUM_LEN;
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
pub const EXECUTE_LOAD_QUERY_HEADER_LEN: u8 =
    (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN);
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

lazy_static! {
    static ref CHECK_SUM_VERSION:Vec<u8> = vec![5, 6, 1];
    static ref CHECK_SUM_VERSION_PRODUCT: u64 = version_product(CHECK_SUM_VERSION.clone());
}

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
            4 => FormatDescriptionDeclare {
                fdv: FormatDescriptionsVersion::V5_0,
            },
            3 => FormatDescriptionDeclare {
                fdv: FormatDescriptionsVersion::V4_0_x,
            },
            1 => FormatDescriptionDeclare {
                fdv: FormatDescriptionsVersion::V3_23,
            },
            _ => {
                error!("unexpected binlog_version: {:x}", binlog_version);
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
    /// Get the post header length for a specific event type
    /// 
    /// # Example
    /// ```ignore
    /// let query_post_header_len = format_desc.get_post_header_len(event_type as usize);
    /// ```
    pub fn get_post_header_len(&self, b_type: usize) -> u8 {
        self.post_header_len[b_type - 1]
    }

    pub fn get_declare(self) -> FormatDescriptionDeclare {
        self.declare.clone()
    }

    pub fn declare(binlog_version: u16) -> Self {
        let mut server_version: String = String::default();
        let mut common_header_len: u8 = LOG_EVENT_HEADER_LEN;
        let mut number_of_event_types: u32 = LOG_EVENT_TYPES as u32;
        let mut post_header_len = vec![0; LogEventType::ENUM_END_EVENT.as_val()];

        let mut create_timestamp: u32 = 0;

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
                post_header_len[FORMAT_DESCRIPTION_EVENT.as_val() - 1] =
                    FORMAT_DESCRIPTION_HEADER_LEN;
                post_header_len[XID_EVENT.as_val() - 1] = XID_HEADER_LEN;
                post_header_len[BEGIN_LOAD_QUERY_EVENT.as_val() - 1] = BEGIN_LOAD_QUERY_HEADER_LEN;
                post_header_len[EXECUTE_LOAD_QUERY_EVENT.as_val() - 1] =
                    EXECUTE_LOAD_QUERY_HEADER_LEN;
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

                post_header_len[TRANSACTION_CONTEXT_EVENT.as_val() - 1] =
                    TRANSACTION_CONTEXT_HEADER_LEN;
                post_header_len[VIEW_CHANGE_EVENT.as_val() - 1] = VIEW_CHANGE_HEADER_LEN;
                post_header_len[XA_PREPARE_LOG_EVENT.as_val() - 1] = XA_PREPARE_HEADER_LEN;
                post_header_len[PARTIAL_UPDATE_ROWS_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[TRANSACTION_PAYLOAD_EVENT.as_val() - 1] =
                    TRANSACTION_PAYLOAD_HEADER_LEN;

                // mariadb 10
                post_header_len[ANNOTATE_ROWS_EVENT.as_val() - 1] = ANNOTATE_ROWS_HEADER_LEN;
                post_header_len[BINLOG_CHECKPOINT_EVENT.as_val() - 1] =
                    BINLOG_CHECKPOINT_HEADER_LEN;
                post_header_len[GTID_EVENT.as_val() - 1] = GTID_HEADER_LEN;
                post_header_len[GTID_LIST_EVENT.as_val() - 1] = GTID_LIST_HEADER_LEN;
                post_header_len[START_ENCRYPTION_EVENT.as_val() - 1] = START_ENCRYPTION_HEADER_LEN;

                // mariadb compress
                post_header_len[QUERY_COMPRESSED_EVENT.as_val() - 1] = QUERY_COMPRESSED_EVENT as u8;
                post_header_len[WRITE_ROWS_COMPRESSED_EVENT.as_val() - 1] = ROWS_HEADER_LEN_V2;
                post_header_len[UPDATE_DELETE_ROWS_COMPRESSED_EVENT.as_val() - 1] =
                    ROWS_HEADER_LEN_V2;
                post_header_len[UPDATE_DELETE_ROWS_COMPRESSED_EVENT.as_val()] = ROWS_HEADER_LEN_V2;
                post_header_len[WRITE_ROWS_COMPRESSED_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                post_header_len[UPDATE_ROWS_COMPRESSED_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
                post_header_len[DELETE_ROWS_COMPRESSED_EVENT_V1.as_val() - 1] = ROWS_HEADER_LEN_V1;
            }
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
                post_header_len[NEW_LOAD_EVENT.as_val() - 1] =
                    post_header_len[LOAD_EVENT.as_val() - 1];
            }
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
                post_header_len[NEW_LOAD_EVENT.as_val() - 1] =
                    post_header_len[LOAD_EVENT.as_val() - 1];
            }
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
}

impl LogEvent for FormatDescriptionEvent {
    fn get_type_name(&self) -> String {
        "FormatDescriptionEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }


    /// ```text
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
    /// ```
    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<FormatDescriptionEvent, ReError> {
        // let declare: FormatDescriptionDeclare = context.get_format_description().get_declare();
        let declare: FormatDescriptionDeclare = FormatDescriptionDeclare::new(4);

        // 一个 u16 代表的 binlog 版本

        let binlog_version = cursor.read_u16::<LittleEndian>()?;
        // 一个固定长度为 50 的字符串（可能包含多个 \0 终止符)
        let mut _server_version_vec = vec![0; ST_SERVER_VER_LEN as usize];
        cursor.read_exact(&mut _server_version_vec)?;
        let server_version = extract_string(&_server_version_vec);

        /* Redundant timestamp & header length which is always 19 */
        // u32 的 timestamp
        let create_timestamp = cursor.read_u32::<LittleEndian>()?;
        // let create_timestamp = 0;
        let common_header_len = cursor.read_u8()?;

        // 一直到事件结尾(去除后面 checksum 和算法)的数组
        // supported_types 要取多少个字节 = header.event_size - 19[header 大小] - (2 + 50 + 4 + 1) - 1[checksum_alg size] - 4[checksum size]
        // 剩下的就是 supported_types 占用的字节数
        let number_of_event_types = header.clone().borrow_mut().get_event_length()
            - (LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_PAYLOAD_WITHOUT_CHECKSUM_LEN) as u32
            - BINLOG_CHECKSUM_ALG_DESC_LEN as u32
            // crc
            - ST_COMMON_PAYLOAD_CHECKSUM_LEN as u32;

        let mut post_header_len = vec![0; number_of_event_types as usize];
        for i in 0..number_of_event_types as usize {
            post_header_len[i] = cursor.read_u8()?;
        }

        let mut checksum_alg = BINLOG_CHECKSUM_ALG_UNDEF;
        let mut checksum_type = ChecksumType::None;
        let split_server_version = server_version_split_with_dot(server_version.clone());
        if version_product(split_server_version) >= *CHECK_SUM_VERSION_PRODUCT {
            let current_pos = cursor.position();
            cursor.set_position((header.clone().borrow_mut().get_event_length()
                - LOG_EVENT_HEADER_LEN as u32
                - BINLOG_CHECKSUM_ALG_DESC_LEN as u32
                - ST_COMMON_PAYLOAD_CHECKSUM_LEN as u32)
                as u64);

            checksum_alg = cursor.read_u8()?;
            checksum_type = ChecksumType::from_code(checksum_alg).unwrap();

            cursor.set_position(current_pos);
        }

        let crc = cursor.read_u32::<LittleEndian>()?;

        let header_new: Header = Header::copy_and_get(header, crc, HashMap::new());

        Ok(
            FormatDescriptionEvent {
                header: header_new,
                binlog_version,
                server_version,
                checksum_type,
                create_timestamp,
                common_header_len,
                post_header_len,
                number_of_event_types,
                declare,
            },
        )
    }
}

/// 将字符串按照 dot 符号切割，并转换为 mysql标准的版本号数字
fn server_version_split_with_dot(server_version: String) -> Vec<u8> {
    let items: Vec<&str> = server_version.split(".").collect();
    if items.len() < 3 {
        return vec![0; 3];
    }

    let mut split = vec![0; 3];

    for i in 0..3 {
        let mut j = 0;
        let v = items[i];
        for char in v.chars() {
            if !char.is_ascii_digit() {
                break;
            }
            j += 1;
        }

        if j > 0 {
            let (number_part, last) = v.split_at(j);
            let _v = number_part.parse::<u8>().unwrap();
            split[i] = _v;
        } else {
            // 非法版本
            split[0] = 0;
            split[1] = 0;
            split[2] = 0;
        }
    }

    split
}

/// 计算当前 version 的值
fn version_product(version_split: Vec<u8>) -> u64 {
    ((version_split[0] as u16 * 256u16 + version_split[1] as u16) as u64 * 256u64 + version_split[2] as u64) as u64
}

#[cfg(test)]
mod test {
    use crate::events::log_context::{ILogContext, LogContext};
    use crate::events::log_position::LogFilePosition;
    use crate::events::protocol::format_description_log_event::{server_version_split_with_dot};

    #[test]
    fn test_server_version_split_with_dot() {
        let mut _context:LogContext = LogContext::new(LogFilePosition::new("AA"));
        _context.update_position_offset(66);

        assert_eq!(server_version_split_with_dot("192.168.9".to_string()), vec![192,168,9]);
        assert_eq!(server_version_split_with_dot("19-a.16B.9".to_string()), vec![19,16,9]);
        assert_eq!(server_version_split_with_dot("19.16.9a".to_string()), vec![19,16,9]);

        assert_eq!(server_version_split_with_dot("19-a.16B.c9".to_string()), vec![0,0,0]);
        assert_eq!(server_version_split_with_dot("a19.16.c9".to_string()), vec![0,0,0]);
        assert_eq!(server_version_split_with_dot("a19.16.9".to_string()), vec![0,16,9]);
        assert_eq!(server_version_split_with_dot("19.a16.9".to_string()), vec![0,0,9]);
        assert_eq!(server_version_split_with_dot("19.16.a9".to_string()), vec![0,0,0]);
    }
}

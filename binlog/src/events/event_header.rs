use nom::{
    bytes::complete::{tag},
    number::complete::{le_i64, le_u16, le_u32, le_u64, le_u8},
    IResult,
};
use serde::Serialize;
use crate::events::event_header_flag::EventFlag;


pub const HEADER_LEN: u8 = 4;

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
    pub server_id: u32,

    /// u32 事件大小， Gets event length (header + event + checksum).
    pub event_length: u32,

    /// Gets file position of next event.  v4版本
    pub log_pos: u32,

    /// Gets event flags.
    /// See <a href="https://mariadb.com/kb/en/2-binlog-event-header/#event-flag">documentation</a>.
    pub flags: u16,
    pub flags_attr: EventFlag,

    ///////////////////////////////////////////////////
    /// other
    ///////////////////////////////////////////////////
    /// checksum_alg
    pub checksum_alg: u8,
    /// checksum
    pub checksum: u32,

    log_file_name : String,
    pub gtid_map : Vec<u8>,
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
            gtid_map: vec![],
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

    pub fn new(log_file_name:String, when: u32, event_type: u8, server_id: u32,
               event_length: u32, log_pos: u32, flags: u16) -> Self {
        let flags_attr = EventFlag::from(flags);

        Header {
            when,
            event_type,
            server_id,
            event_length,
            log_pos,
            flags,
            flags_attr,
            checksum_alg: 0,
            checksum: 0,
            log_file_name,
            gtid_map: vec![],
        }
    }

    /// binlog文件以一个值为0Xfe62696e的魔数开头，这个魔数对应0xfe 'b''i''n'。
    pub fn check_start(i: &[u8]) -> IResult<&[u8], &[u8]> {
        tag([254, 98, 105, 110])(i)
    }

    /// 解析 header
    pub fn parse_v4_header<'a>(input: &'a [u8]) -> IResult<&'a [u8], Header> {
        let (i, timestamp) = le_u32(input)?;
        let (i, event_type) = le_u8(i)?;
        let (i, server_id) = le_u32(i)?;
        let (i, event_length) = le_u32(i)?;
        let (i, log_pos) = le_u32(i)?;

        // 计算 flags
        let (i, flags) = le_u16(i)?;

        Ok((
            i,
            Header::new("".to_string(), timestamp, event_type, server_id, event_length, log_pos, flags),
        ))
    }

    pub fn copy(source: &Header) -> Self  {
        let log_file_name : String = source.get_log_file_name();

        Header {
            when: source.when,
            event_type: source.event_type,
            server_id: source.server_id,
            event_length: source.event_length,
            log_pos: source.log_pos,
            flags: source.flags,
            flags_attr: source.get_flags_attr(),
            checksum_alg: source.checksum_alg,
            checksum: source.checksum,
            log_file_name,
            gtid_map: source.gtid_map.clone(),
        }
    }

    pub fn copy_and_get(source: &Header, checksum_alg: u8, checksum: u32, gtid_map : Vec<u8>) -> Self  {
        let log_file_name : String = source.get_log_file_name();

        Header {
            when: source.when,
            event_type: source.event_type,
            server_id: source.server_id,
            event_length: source.event_length,
            log_pos: source.log_pos,
            flags: source.flags,
            flags_attr: source.get_flags_attr(),
            checksum_alg,
            checksum,
            log_file_name,
            gtid_map,
        }
    }
}

// impl<I: InputBuf> Decode<I> for EventHeader {
//     fn decode(input: &mut I) -> Result<Self, DecodeError> {
//         let timestamp = Int4::decode(input)?;
//         let event_type = Int1::decode(input)?;
//         let server_id = Int4::decode(input)?;
//         let event_size = Int4::decode(input)?;
//         let log_pos = Int4::decode(input)?;
//         let flags = EventHeaderFlag::decode(input)?;
//
//         Ok(Self {
//             timestamp,
//             event_type,
//             server_id,
//             event_size,
//             log_pos,
//             flags,
//         })
//     }
// }

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}

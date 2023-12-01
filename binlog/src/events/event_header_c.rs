// use common::codec::{Decode};
// use common::parse::parse::InputBuf;
// use crate::event_header_flag::EventHeaderFlag;
//
// /////////////////////////////////////
// ///  EventHeader Header
// /// 每个event都有一个19个字节的Binlog Event Header
// ///
// /// binlog 采用小端序列，也就说 server-id 为1 的u32 应该为 0x01 0x00 0x00 0x00。
// /////////////////////////////////////
// #[derive(Debug, Serialize, PartialEq, Eq, Clone)]
// #[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
// pub struct EventHeader {
//     pub timestamp: u32,  // 4 个字节的timestamp
//     pub event_type: u8,  // 1 byte
//     pub server_id: u32,  // 4 byte， 该id表明binlog的源server是哪个，用来在循环复制 binlog event）
//     pub event_size: u32,  // 4 byte, event包大小
//     pub log_pos: u32,  // 4 byte, 下一个event起始偏移 next_posotion
//     pub flags: EventHeaderFlag,  // u16, 2 byte
// }

// /// 解析 header
// /// 首先要读取一个4字节的 timestamp, 就是 u32,
// /// 接着是1个代表事件类型的 u8, 而后是 u32 的 server_id,
// /// u32 事件大小，
// /// u32 log position(我们只解析v4版本，所以log position一定存在) 和 u16 flag。
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
//
// /// header 结构，我们首先要读取一个4字节的 timestamp, 就是 u32, 接着是1个代表事件类型的 u8, 而后是 u32 的 server_id, u32 事件大小，u32 log position(我们只解析v4版本，所以log position一定存在) 和 u16 flag。
// ///
// /// nom 提供了 le_u32, le_u8, le_u16 等小端整数解析函数，因此 按照这个描述，header 解析函数可以写为
// pub fn parse_header(input: &[u8]) -> IResult<&[u8], Header> {
//     let (i, timestamp) = le_u32(input)?;
//     let (i, event_type) = le_u8(i)?;
//     let (i, server_id) = le_u32(i)?;
//     let (i, event_size) = le_u32(i)?;
//     let (i, log_pos) = le_u32(i)?;
//     let (i, flags) = map(le_u16, |f: u16| EventFlag {
//         in_use: (f >> 0) % 2 == 1,
//         forced_rotate: (f >> 1) % 2 == 1,
//         thread_specific: (f >> 2) % 2 == 1,
//         suppress_use: (f >> 3) % 2 == 1,
//         update_table_map_version: (f >> 4) % 2 == 1,
//         artificial: (f >> 5) % 2 == 1,
//         relay_log: (f >> 6) % 2 == 1,
//         ignorable: (f >> 7) % 2 == 1,
//         no_filter: (f >> 8) % 2 == 1,
//         mts_isolate: (f >> 9) % 2 == 1,
//     })(i)?;
//     Ok((
//         i,
//         Header {
//             timestamp,
//             event_type,
//             server_id,
//             event_size,
//             log_pos,
//             flags,
//         },
//     ))
// }

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}

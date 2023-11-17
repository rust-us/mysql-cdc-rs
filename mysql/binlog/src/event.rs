// use crate::binlog::event_type::EventType;
//
//
// #[derive(Debug, Clone)]
// #[cfg_attr(feature = "serde", serde::Serialize, serde::DeSerialize)]
// pub struct EventHeader {
//     pub timestamp: Int4,
//     pub event_type: Int1,
//     pub server_id: Int4,
//     pub event_size: Int4,
//     pub log_pos: Int4,
//     pub flags: EventHeaderFlag,
// }
//
// /// binlog file 内的 event data
// pub struct BinlogEvent {
//     /// 事件类型，Format_desc、Query、Table_map、Write_rows、Xid、Update_rows、Delete_rows...
//     event_type: EventType,
//
//     /// 数据的开始 pos, 包含
//     start_log_pos: i32,
//
//     /// 数据的结束 pos, 不包含
//     end_log_pos : i32,
//
//     /// 数据
//     info : String,
//
// }
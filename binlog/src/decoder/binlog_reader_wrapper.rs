// use common::err::DecodeError::ReError;
// use crate::decoder::binlog_decoder::{BinlogReader, FileBinlogReader};
// use crate::events::event::Event;
//
// #[derive(Debug)]
// pub struct BinlogReaderWrapper {
//     pub decoder: FileBinlogReader,
//
// }
//
// pub struct EventDetail {
//     name :String,
//     event :Event,
// }
//
// impl BinlogReaderWrapper {
//     pub fn new(decoder: FileBinlogReader) -> Self {
//         BinlogReaderWrapper {
//             decoder,
//         }
//     }
//
//     pub fn get_events(&self) -> Result<Vec<EventDetail>, ReError> {
//         let mut event_list = Vec::<EventDetail>::new();
//         if self.decoder.eof() {
//             return Ok(event_list);
//         }
//
//         for result in self.decoder.read_events() {
//             let (header, event) = result.unwrap();
//
//             event_list.push(EventDetail::new(Event::get_type_name(&event), event));
//         }
//
//         Ok(event_list)
//     }
//
//     // pub fn iter(&self) -> Result<dyn Iterator<Item=(EventDetail)>, ReError>
// }
//
// impl EventDetail {
//     pub fn new(name: String, event: Event) -> Self {
//         EventDetail {
//             name,
//             event,
//         }
//     }
// }
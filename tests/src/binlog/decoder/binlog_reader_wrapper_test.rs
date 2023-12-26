// use std::fs::{File, OpenOptions};
// use std::path::Path;
// use binlog::decoder::binlog_decoder::{BinlogReader, FileBinlogReader};
// use binlog::decoder::binlog_reader_wrapper::BinlogReaderWrapper;
//
// #[test]
// fn test_table_map() {
//     let file = load_read_only_file("C:/Workspace/test_data/8.0/19_30_Table_map_event_Write_rows_log_event/binlog.000018");
//
//     let reader = FileBinlogReader::new(file).unwrap();
//
//     let wrapper = BinlogReaderWrapper::new(reader);
//
//     let list = wrapper.get_events().unwrap();
//     assert!(list.len() > 0);
// }
//
// fn load_read_only_file(name: &str) -> File {
//     let path = Path::new(name);
//
//     OpenOptions::new()
//         .read(true)
//         // .append(true)
//         // .write(true)
//         // .create(true)
//         .open(path)
//         .unwrap()
// }
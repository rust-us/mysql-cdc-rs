use std::fs::{File, OpenOptions};
use std::path::Path;
use binlog::decoder::binlog_decoder::{BinlogReader};
use binlog::decoder::file_binlog_reader::FileBinlogReader;
use binlog::events::event::Event;

#[test]
fn test_read_events() {
    let file = load_read_only_file("C:/Workspace/test_data/8.0/02_query/binlog.000001");

    let (reader, context) = FileBinlogReader::new_without_context(false).unwrap();

    for result in reader.read_events(file) {
        let (header, event) = result.unwrap();
        println!("============================ {}", Event::get_type_name(&event));
        println!("{:#?}", header);
        println!("{:#?}", event);
        println!("");
        assert!(header.event_length > 0);
    }

    fn load_read_only_file(name: &str) -> File {
        let path = Path::new(name);
        let exists = path.exists();

        OpenOptions::new()
            .read(true)
            // .append(true)
            // .write(true)
            // .create(true)
            .open(path)
            .unwrap()
    }
}
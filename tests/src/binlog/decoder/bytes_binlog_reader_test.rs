use binlog::decoder::binlog_decoder::{BinlogReader};
use binlog::decoder::bytes_binlog_reader::BytesBinlogReader;
use binlog::events::event::Event;

#[test]
fn test_read_events() {
    let input = include_bytes!("../../../events/5.7/15_format_desc/log.bin");

    let (reader, context) = BytesBinlogReader::new_without_context(false).unwrap();

    let mut idx = 0;
    for result in reader.read_events(input) {
        let event = result.unwrap();
        println!("============================ {}", Event::get_type_name(&event));

        idx += 1;
    }
    assert_eq!(idx, 3);
}
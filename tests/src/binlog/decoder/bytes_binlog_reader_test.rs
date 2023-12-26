use binlog::decoder::binlog_decoder::{BinlogReader, BytesBinlogReader};
use binlog::events::event::Event;

#[test]
fn test_read_events() {
    let input = include_bytes!("../../../events/5.7/15_format_desc/log.bin");

    let reader = BytesBinlogReader::new(input).unwrap();

    let mut idx = 0;
    for result in reader.read_events() {
        let event = result.unwrap();
        println!("============================ {}", Event::get_type_name(&event));

        idx += 1;
    }
    assert_eq!(idx, 3);
}
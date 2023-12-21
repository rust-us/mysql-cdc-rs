
#[cfg(test)]
mod test_normal {
    use std::fs::{File, OpenOptions};
    use std::path::Path;
    use binlog::decoder::binlog_decoder::{BinlogReader, FileBinlogReader};
    use binlog::events::event::Event;
    use common::log::log_factory::LogFactory;

    #[test]
    fn test() {
        LogFactory::init_log(true);

        println!("test");
    }

    #[test]
    fn test_query_default() {
        let file = load_read_only_file("C:/Workspace/stoneatom/Replayer/tests/events/8.0/02_query/binlog.000001");

        let reader = FileBinlogReader::new(file).unwrap();

        for result in reader.read_events() {
            let (header, event) = result.unwrap();
            println!("============================ {}", Event::get_type_name(&event));
            println!("{:#?}", header);
            println!("{:#?}", event);
            println!("");
            assert!(header.event_length > 0);
        }
    }

    #[test]
    fn test_table_map() {
        let file = load_read_only_file("C:/Workspace/stoneatom/Replayer/tests/events/8.0/19_30_Table_map_event_Write_rows_log_event/binlog.000018");

        let reader = FileBinlogReader::new(file).unwrap();

        for result in reader.read_events() {
            let (header, event) = result.unwrap();
            // println!("============================ {}", Event::get_type_name(&event));
            // println!("{:#?}", header);
            // println!("{:#?}", event);
            // println!("");
            assert!(header.event_length > 0);
        }
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
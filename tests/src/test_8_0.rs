
#[cfg(test)]
mod test_normal {
    use std::fs::{File, OpenOptions};
    use std::path::Path;
    use binlog::decoder::binlog_decoder::{BinlogReader, FileBinlogReader};
    use common::log::log_factory::LogFactory;

    #[test]
    fn test() {
        LogFactory::init_log(true);

        println!("test");
    }

    #[test]
    fn test_file_reader() {
        let file = load_read_only_file("C:/Workspace/ali/Replayer/tests/events/8.0/02_query/binlog.000001");

        let reader = FileBinlogReader::new(file).unwrap();

        for result in reader.read_events() {
            let (header, event) = result.unwrap();
            // println!("{:#?}", header);
            // println!("{:#?}", event);
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
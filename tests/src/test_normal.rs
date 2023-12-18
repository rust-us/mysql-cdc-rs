
#[cfg(test)]
mod test_normal {
    use binlog::events::event::Event::{Query, RowQuery, Stop};
    use binlog::events::event_factory::EventFactory;
    use common::log::log_factory::LogFactory;

    #[test]
    fn test() {
        println!("test");
    }

    #[test]
    fn test_total_v8_1() {
        LogFactory::init_log(true);

        let mut input = include_bytes!("../events/8.0/binlog.000001");

        let (i, output) = EventFactory::from_bytes_with_context(input).unwrap();
        match output.get(3).unwrap() {
            Query { .. } => {},
            _ => panic!("should be row_query"),
        }
    }

}
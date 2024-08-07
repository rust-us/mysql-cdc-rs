
#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;
    use tracing::debug;
    use binlog::decoder::event_decoder::LogEventDecoder;
    use binlog::events::binlog_event::BinlogEvent;
    use binlog::events::event_raw::EventRaw;
    use binlog::factory::event_factory::{EventFactory, EventReaderOption, IEventFactory};
    use binlog::events::event_header::Header;
    use binlog::events::log_context::{ILogContext, LogContext, LogContextRef};
    use binlog::events::log_position::LogFilePosition;
    use common::err::decode_error::ReError;
    use common::log::tracing_factory::TracingFactory;
    use crate::binlog::factory::test_iter_owener::TestOwenerIter;

    ///EventRaw 转为 Event
    fn event_raw_to_event(raw: &EventRaw, context: LogContextRef) -> Result<BinlogEvent, ReError> {
        let mut decoder = LogEventDecoder::new();

        let header = raw.get_header();
        let full_packet = raw.get_payload();
        decoder.event_parse(full_packet, header, context)
    }

    #[test]
    fn test() {
        TracingFactory::init_log(true);

        debug!("test");
    }

    #[test]
    fn test_event_raw_to_event() {
        let bytes = include_bytes!("../../../events/8.0/02_query/binlog.000001");

        let (i, _) = Header::check_start(bytes).unwrap();

        let mut _context:LogContext = LogContext::default();
        &_context.force_set_log_position(LogFilePosition::new("test"));
        let context = Rc::new(RefCell::new(_context));

        let (i, event_raws) = EventRaw::steam_to_event_raw(i, context.clone()).unwrap();
        assert_eq!(i.len(), 0);
        assert_eq!(event_raws.len(), 4);

        let mut event_list = Vec::<BinlogEvent>::with_capacity(event_raws.len());
        for event_raw in event_raws {
            let rs = event_raw_to_event(&event_raw, context.clone());

            match rs {
                Err(e) => {
                    // todo , ignore
                },
                Ok(e) => {
                    event_list.push(e);
                }
            }
        }
        assert_eq!(event_list.len(), 4);
    }

    #[test]
    fn test_parser_bytes_remaing() {
        let bytes = include_bytes!("../../../events/8.0/02_query/binlog.000001");

        let len = bytes.len();
        assert_eq!(len, 369);

        let part1 = Vec::from(&bytes[0..(len - 60)]);
        let part2 = Vec::from(&bytes[(len - 60)..]);
        assert_eq!(part1.len(), 309);
        assert_eq!(part2.len(), 60);

        let mut factory = EventFactory::new(false);

        let (remaing, event_list) = factory.parser_bytes(&part1, &EventReaderOption::default()).unwrap();
        assert_eq!(remaing.len(), 75);
        assert_eq!(event_list.len(), 3);
    }

    #[test]
    fn test_parser_iter_remaing() {
        let bytes = include_bytes!("../../../events/8.0/02_query/binlog.000001");

        let len = bytes.len();
        assert_eq!(len, 369);

        // 0- 100
        let part1 = Vec::from(&bytes[0..(len - 269)]);
        // 100 - 199
        let part2 = Vec::from(&bytes[(len - 269)..(len - 170)]);
        // 199 - 369
        let part3 = Vec::from(&bytes[(len - 170)..]);

        let mut data_iter = Vec::new();
        data_iter.push(part1);
        data_iter.push(part2);
        data_iter.push(part3);

        // let iter = TestRefIter::new(data_iter);
        let iter = TestOwenerIter::new(data_iter);

        let mut factory = EventFactory::new(false);

        factory.parser_iter(iter.iter(), &EventReaderOption::debug());

        // 总计4个事件
        let binding = factory.get_context();
        let context = binding.borrow();
        let format_description = context.get_format_description();
    }

    /// 验证上下文指针的正确性
    #[test]
    fn test_context_position_check() {
        let bytes = include_bytes!("../../../events/8.0/02_query_bigger/binlog.000733");

        let len = bytes.len();
        assert_eq!(len, 7843);

        // 0- 1000， len 1000
        let part1 = Vec::from(&bytes[0..1000]);
        assert_eq!(part1.len(), 1000);
        // 1000 - 2500， len 1500
        let part2 = Vec::from(&bytes[1000..2500]);
        assert_eq!(part2.len(), 1500);
        // 2500 - 3915， len 1415
        let part3 = Vec::from(&bytes[2500..]);
        assert_eq!(part3.len(), 5343);

        let mut data_iter = Vec::new();
        data_iter.push(part1);
        data_iter.push(part2);
        data_iter.push(part3);

        let iter = TestOwenerIter::new(data_iter);
        let mut factory = EventFactory::new(false);

        factory.parser_iter(iter.iter(), &EventReaderOption::debug());

        // 总计4个事件
        let binding = factory.get_context();
        let context = binding.borrow();
        let format_description = context.get_format_description();

        assert_eq!(context.get_position_offset(), len as u64);
    }
}

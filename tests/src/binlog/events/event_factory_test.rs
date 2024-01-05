
#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, RwLock};
    use binlog::events::event::Event;
    use binlog::events::event_factory::EventFactory;
    use binlog::events::event_header::Header;
    use binlog::events::log_context::LogContext;
    use binlog::events::log_position::LogPosition;
    use common::log::log_factory::LogFactory;

    #[test]
    fn test() {
        LogFactory::init_log(true);

        println!("test");
    }

    #[test]
    fn test_steam_to_event_raw() {
        let bytes = include_bytes!("../../../events/8.0/02_query/binlog.000001");

        let (i, _) = Header::check_start(bytes).unwrap();

        let mut _context:LogContext = LogContext::default();
        &_context.set_log_position(LogPosition::new("test"));
        let context = Rc::new(RefCell::new(_context));

        let (i, event_raws) = EventFactory::steam_to_event_raw(i, context).unwrap();
        assert_eq!(i.len(), 0);
        assert_eq!(event_raws.len(), 4);
    }

    #[test]
    fn test_event_raw_to_event() {
        let bytes = include_bytes!("../../../events/8.0/02_query/binlog.000001");

        let (i, _) = Header::check_start(bytes).unwrap();

        let mut _context:LogContext = LogContext::default();
        &_context.set_log_position(LogPosition::new("test"));
        let context = Rc::new(RefCell::new(_context));

        let (i, event_raws) = EventFactory::steam_to_event_raw(i, context.clone()).unwrap();
        assert_eq!(i.len(), 0);
        assert_eq!(event_raws.len(), 4);

        let mut event_list = Vec::<Event>::with_capacity(event_raws.len());
        for event_raw in event_raws {
            let rs = EventFactory::event_raw_to_event(&event_raw, context.clone());

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
}
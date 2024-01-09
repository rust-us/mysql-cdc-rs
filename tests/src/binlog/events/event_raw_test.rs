
#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;
    use binlog::events::event_raw::EventRaw;
    use binlog::events::event_header::Header;
    use binlog::events::log_context::{ILogContext, LogContext};
    use binlog::events::log_position::LogPosition;

    #[test]
    fn test_steam_to_event_raw() {
        let bytes = include_bytes!("../../../events/8.0/02_query/binlog.000001");

        let (i, _) = Header::check_start(bytes).unwrap();

        let mut _context:LogContext = LogContext::default();
        &_context.set_log_position(LogPosition::new("test"));
        let context = Rc::new(RefCell::new(_context));

        let (i, event_raws) = EventRaw::steam_to_event_raw(i, context).unwrap();
        assert_eq!(i.len(), 0);
        assert_eq!(event_raws.len(), 4);
    }

}
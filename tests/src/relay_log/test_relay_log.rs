use tracing::info;
use binlog::events::binlog_event::BinlogEvent;
use binlog::factory::event_factory::{EventFactory, EventReaderOption, IEventFactory};
use common::log::tracing_factory::TracingFactory;
use relay_log::relay_log::RelayLog;


pub fn get_table_map_event_write_rows_log_event() -> Vec<BinlogEvent> {
    let input = include_bytes!("../../events/8.0/19_30_Table_map_event_Write_rows_log_event/binlog.000018");
    let mut factory = EventFactory::new(false);
    let (_, output) = factory.parser_bytes(input, &EventReaderOption::default()).unwrap();
    output
}

#[test]
pub fn test_binlog_event_to_relay_entity() {
    TracingFactory::init_log(true);
    let local_events = get_table_map_event_write_rows_log_event();
    let relay_entities: Vec<RelayLog> = local_events.iter().map(|e| {
        RelayLog::from_binlog_event(e)
    }).collect();

    info!("local_events: {:?}", local_events);
    info!("relay_entities: {:?}", relay_entities);
}
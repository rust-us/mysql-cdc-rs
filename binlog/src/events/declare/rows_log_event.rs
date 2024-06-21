use common::err::decode_error::ReError;
use crate::events::event_header::Header;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;

pub trait RowsLogEvent {
    /// 事件名
    fn fill_assembly_table(&mut self, context: LogContextRef) -> Result<bool, ReError>;

    fn get_table_map_event(&self) -> Option<&TableMapEvent>;

    /// 通过 =复制，得到 Header
    fn get_header(&self) -> Header;
}
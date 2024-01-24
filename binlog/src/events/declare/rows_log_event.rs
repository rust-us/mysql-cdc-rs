use common::err::decode_error::ReError;
use crate::events::log_context::LogContextRef;

pub trait RowsLogEvent {
    /// 事件名
    fn fill_assembly_table(&mut self, context: LogContextRef) -> Result<bool, ReError>;

}
use common::config::BinlogConfig;
use common::err::CResult;
use common::err::decode_error::ReError;
use common::server::Server;
use crate::binlog::binlog_events_wrapper::{BinlogEventsWrapper};

#[async_trait::async_trait]
pub trait BinlogLifecycle: Server {
    /// 初始化
    async fn setup(&mut self, binlog_config: &BinlogConfig) -> CResult<()>;

    async fn binlogs(&mut self) -> Result<BinlogEventsWrapper, ReError>;

    /// 暂停服务，服务挂起
    async fn pause(&mut self) -> CResult<()>;


}
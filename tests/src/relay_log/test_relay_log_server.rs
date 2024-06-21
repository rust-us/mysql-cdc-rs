use std::time::Duration;

use tokio::join;
use tokio::sync::mpsc;
use tracing::{info, warn};

use binlog::events::binlog_event::BinlogEvent;
use common::err::CResult;
use common::log::tracing_factory::TracingFactory;
use relay_log::relay_log_server::RelayLogServer;

use crate::relay_log::test_relay_log;

#[tokio::test]
pub async fn test_server() -> CResult<()>{
    TracingFactory::init_log(true);

    let events = test_relay_log::get_table_map_event_write_rows_log_event();

    // 多发送端，单一接收端通道
    let (tx, rx) = mpsc::channel::<BinlogEvent>(10);

    let server = RelayLogServer::new_with_binlog_receiver(rx)?;

    info!("{:?}", server);

    let send_task = tokio::spawn(async move {
        for e in events.into_iter() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tx.send(e).await.is_err() {
                warn!("receiver closed");
                break;
            }
        }
    });

    info!("before relay log server state: {}", server.is_running().unwrap());

    join!(send_task);

    info!("after relay log server state: {}", server.is_running().unwrap());

    tokio::time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
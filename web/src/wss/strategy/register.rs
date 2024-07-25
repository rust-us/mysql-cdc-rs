use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use common::server::Server;
use crate::web_error::WResult;
use crate::wss::session::WssSessionRef;
use crate::wss::strategy::WSSStrategy;

pub struct StartBinlogStrategyEvent {
    _inner_data: HashMap<String, String>,

    session: WssSessionRef,
}

impl WSSStrategy for StartBinlogStrategyEvent {
    fn action(&mut self, rt: Arc<Runtime>) -> WResult<Option<String>> {
        // // 使用 tokio::spawn 并发执行异步任务
        // rt.block_on(self.session.lock().unwrap().start());
        tokio::runtime::Builder::new_multi_thread()
            // 启用所有tokio特性， 如 IO 和定时器服务
            .enable_all()
            .build().unwrap()
            .block_on(async {
                let mut session_lock = self.session.lock(); // 对于 RwLock，使用 try_lock 或 lock 并处理 Result
                session_lock.unwrap().start().await; // start() 是异步的
            });

        Ok(Some("StartBinlog start success".to_string()))
    }

    fn code(&self) -> i16 {
        1
    }
}

impl StartBinlogStrategyEvent {
    pub fn new(session: WssSessionRef, _inner_data: HashMap<String, String>) -> Self {
        StartBinlogStrategyEvent {
            _inner_data,
            session,
        }
    }
}
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::runtime::Runtime;
use common::err::decode_error::ReError;
use common::server::Server;
use crate::web_error::WResult;
use crate::wss::strategy::ignore::IgnoreStrategyEvent;
use crate::wss::strategy::register::StartBinlogStrategyEvent;
use crate::wss::strategy::unknow::UnknownStrategyEvent;
use crate::wss::strategy::WSSStrategy;
use crate::wss::wss_action_type::ActionType;
use crate::wss::session::{WssSession, WssSessionRef};

#[derive(Debug)]
pub struct WSSFactory {
    session: WssSessionRef
}

impl WSSFactory {
    /// server: Rc<RefCell<MyWebSocket>>
    pub fn create() -> Self {
        let s = WssSession::default();

        WSSFactory {
            session: Arc::new(Mutex::new(s)),
        }
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        self.session.lock().unwrap().is_ready()
    }

    pub fn strategy_action(&self, rt: Arc<Runtime>, action: ActionType, data: HashMap<String, String>) -> WResult<Option<String>> {
        let mut strategy = self.strategy(action, data);

        return strategy.action(rt);
    }

    fn strategy(&self, action: ActionType, data: HashMap<String, String>) -> Box<dyn WSSStrategy> {
        let s: Box<dyn WSSStrategy> = match action {
            ActionType::StartBinlog => {
                Box::new(StartBinlogStrategyEvent::new(self.session.clone(), data))
            },
            ActionType::IGNORE => {
                Box::new(IgnoreStrategyEvent::new())
            },
            // contains: ActionType::UNKNOW、ActionType::CONNECTION
            _ => {
                Box::new(UnknownStrategyEvent::new(data))
            }
        };

        return s
    }
}
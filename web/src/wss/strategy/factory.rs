use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use common::err::decode_error::ReError;
use common::server::Server;
use crate::web_error::WResult;
use crate::wss::event::WSEvent;
use crate::wss::strategy::ignore::IgnoreStrategyEvent;
use crate::wss::strategy::register::RegisterStrategyEvent;
use crate::wss::strategy::unknow::UnknowStrategyEvent;
use crate::wss::strategy::WSSStrategy;
use crate::wss::wss_action_type::ActionType;
use crate::wss::wsscontext::WSSContext;

#[derive(Clone)]
pub struct WSSFactory {
    context: Rc<RefCell<WSSContext>>
}

unsafe impl Send for WSSFactory {}

#[async_trait::async_trait]
impl Server for WSSFactory {
    async fn start(&mut self) -> Result<(), ReError> {
       todo!()
    }

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError> {
        todo!()
    }
}

impl WSSFactory {
    /// server: Rc<RefCell<MyWebSocket>>
    pub fn create() -> Self {
        let c = WSSContext::default();

        WSSFactory {
            context: Rc::new(RefCell::new(c)),
        }
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        self.context.borrow().is_ready()
    }

    pub fn strategy(&self, action: ActionType, data: HashMap<String, String>) -> WResult<Option<String>> {
        let strategy = self.get_strategy_with(action, data);

        let rs : WResult<Option<String>> = strategy.action();
        return rs;
    }

    fn get_strategy_with(&self, action: ActionType, data: HashMap<String, String>) -> Box<dyn WSSStrategy> {
        let s: Box<dyn WSSStrategy> = match action {
            ActionType::CONNECTION => {
                Box::new(RegisterStrategyEvent::new(data))
            }
            ActionType::IGNORE => {
                Box::new(IgnoreStrategyEvent::new())
            }
            ActionType::UNKNOW => {
                Box::new(UnknowStrategyEvent::new(data))
            }
        };

        return s
    }

    fn get_strategy(&self, e: &WSEvent) -> Box<dyn WSSStrategy> {
        let t = ActionType::try_from(e.get_action()).unwrap();

        self.get_strategy_with(t, e.get_body())
    }
}
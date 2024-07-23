use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use common::err::decode_error::ReError;
use common::server::Server;
use crate::web_error::WResult;
use crate::wss::event::WSEvent;
use crate::wss::server::MyWebSocket;
use crate::wss::strategy::Ignore::IgnoreStrategyEvent;
use crate::wss::strategy::register::RegisterStrategyEvent;
use crate::wss::strategy::unknow::UnknowStrategyEvent;
use crate::wss::strategy::WSSStrategy;
use crate::wss::WSSContext::WSSContext;

#[derive(Clone)]
pub struct WSSFactory {
    server: Rc<RefCell<MyWebSocket>>,

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
            server: Rc::new(RefCell::new(MyWebSocket::new())),
            context: Rc::new(RefCell::new(c)),
        }
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        self.context.borrow().is_ready()
    }

    pub fn strategy_with(&self, action: i16, data: HashMap<String, String>) -> WResult<Option<String>> {
        let strategy = self.get_strategy_with(action, data);

        let rs : WResult<Option<String>> = strategy.action();

        return match rs {
            Ok(o) => {
                match o {
                    None => {
                        Ok(None)
                    }
                    Some(msg) => {
                        if action == 0 {
                            Ok(Some(format!("Binlog Server 连接成功。{}", msg)))
                        } else {
                            Ok(Some(msg))
                        }
                    }
                }
            }
            Err(err) => {
                Err(err)
            }
        }
    }

    pub fn strategy(&self, e: &WSEvent) -> WResult<Option<String>> {
        self.strategy_with(e.get_action(), e.get_body())
    }

    fn get_strategy_with(&self, action: i16, data: HashMap<String, String>) -> Box<dyn WSSStrategy> {
        if action == 0 {
            return Box::new(IgnoreStrategyEvent::new());
        }else if action == 1 {
            return Box::new(RegisterStrategyEvent::new(data));
        }

        return Box::new(UnknowStrategyEvent::new(data));
    }

    fn get_strategy(&self, e: &WSEvent) -> Box<dyn WSSStrategy> {
        self.get_strategy_with(e.get_action(), e.get_body())
    }
}
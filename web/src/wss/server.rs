use std::sync::Arc;
use std::time::{Duration, Instant};
use actix::Message;

use actix::prelude::*;
use actix_http::ws::{CloseCode, CloseReason};
use actix_web_actors::ws;
use tokio::runtime::Runtime;
use crate::web_error::{WebError, WResult};
use crate::wss::event::WSEvent;
use crate::wss::session_manager::SessionManager;
use crate::wss::strategy::factory::WSSFactory;
use crate::wss::wss_action_type::ActionType;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub struct MyWebSocket {
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT),
    /// otherwise we drop connection.
    hb: Instant,

    fatory: Option<Arc<WSSFactory>>,
    
    session_id: Option<String>,

    /// 一个 `current_thread` 模式的 `tokio` 运行时，
    /// 使用阻塞的方式来执行异步的操作
    rt: Arc<Runtime>,
}

impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<MyWebSocket>;

    /// Method is called on actor start. We start the heartbeat process here.
    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);
    }
}

/// Handler for ws::Message message: `ws::Message`
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWebSocket {
    /// process websocket messages
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        // 接收到信息
        match msg {
            // Ping message.
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            // Pong message.
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            // Text message. 信息的响应
            Ok(ws::Message::Text(text)) => {
                let e = WSEvent::parser(text.to_string());

                match e {
                    Ok(event) => {
                        let result_msg = self.exec_action(&event);

                        let msg = match result_msg {
                            Ok(m) => {
                                m
                            }
                            Err(err) => {
                                Some(err.to_string())
                            }
                        };

                        match msg {
                            None => {}
                            Some(resp) => {
                                ctx.text(resp);
                            }
                        }
                    }
                    Err(err) => {
                        let msg = format!("Cause : {}", err.to_string());
                        println!("{}", &msg);

                        self.ctx_close(ctx, Some(
                            CloseReason::from((CloseCode::Abnormal, msg))
                        ));
                    }
                }
            },
            // Binary message.
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            // Close message with optional reason.
            Ok(ws::Message::Close(reason)) => {
                match &reason {
                    None => {}
                    Some(m) => {
                        eprintln!("close ws reason: {:?}", m);
                    }
                }

                self.ctx_close(ctx, reason);
            },
            _ => (),
            // _ =>  self.ctx_close(ctx, None);,
        }
    }
}

impl MyWebSocket {
    pub fn new(session_id: Option<String>) -> Self {
        // 构建一个 tokio 运行时： Runtime
        let rt =
            // 同步方法中调用异步的连接方法。 同步等待。
            // 由于 current_thread 运行时并不生成新的线程，只是运行在已有的主线程上，因此只有当 block_on 被调用后，该运行时才能执行相应的操作。
            // 一旦 block_on 返回，那运行时上所有生成的任务将再次冻结，直到 block_on 的再次调用。
            // tokio::runtime::Builder::new_current_thread()
            // 使用多线程模式
            tokio::runtime::Builder::new_multi_thread()
                // 启用所有tokio特性， 如 IO 和定时器服务
            .enable_all()
            .build().unwrap();

        Self {
            hb: Instant::now(),
            fatory: None,
            session_id,
            rt: Arc::new(rt),
        }
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        self.fatory.is_some() && self.fatory.as_ref().unwrap().is_ready()
    }

    fn setup(&mut self) -> () {
        if !self.is_ready() {
            let factory = WSSFactory::create();
            self.fatory = Some(Arc::new(factory));

            self.do_send("Register setup Success").unwrap();
        }
    }

    fn do_send(&self, msg: &str) -> WResult<bool> {
        return match self.session_id.as_ref() {
            None => {
                Ok(false)
            }
            Some(cid) => {
                let context = SessionManager::ws_get(cid);

                match context {
                    None => {
                        Ok(false)
                    }
                    Some(ws_context) => {
                        ws_context.do_send(msg);

                        Ok(true)
                    }
                }
            }
        }
    }

    /// helper method that sends ping to client every 5 seconds (HEARTBEAT_INTERVAL).
    ///
    /// also this method checks heartbeats from client
    fn heartbeat(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // heartbeat timed out
                println!("Websocket Client heartbeat failed, disconnecting!");

                // stop actor
                ctx.stop();

                // don't try to send a ping
                return;
            }

            ctx.ping(b"");
        });
    }

    fn ctx_close(&self, ctx: &mut <Self as Actor>::Context, reason: Option<CloseReason>) {
        if self.session_id.is_some() {
            let key = self.session_id.as_ref().unwrap().as_str();
            let _context: Option<Arc<WsContext>> = SessionManager::ws_remove(key);
            println!("ctx_close {:?}", _context);
        }

        ctx.close(reason);

        ctx.stop();
    }

    /// 执行策略
    fn exec_action(&mut self, e: &WSEvent) -> WResult<Option<String>> {
        let mut action = ActionType::try_from(e.get_action())?;

        match action {
            ActionType::CONNECTION => {
                self.setup();
                action = ActionType::StartBinlog;
            }
            _ => {}
        }

        if self.is_ready() {
            return self.fatory.as_ref().unwrap().strategy_action(self.rt.clone(), action, e.get_body());
        }
        return Err(WebError::Value("Server is not ready".to_string()));
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SendMessage(pub String);

impl SendMessage {
    pub fn new(msg: String) -> Self {
        SendMessage(msg)
    }
}

impl Handler<SendMessage> for MyWebSocket {
    type Result = ();

    fn handle(&mut self, msg: SendMessage, ctx: &mut Self::Context) -> Self::Result {
        ctx.text(msg.0);
    }
}

#[derive(Debug)]
pub struct WsContext {
    addr: Addr<MyWebSocket>,

    cid: String,

    create_time: String,
}

impl WsContext {
    pub fn new(addr: Addr<MyWebSocket>, cid: String, now: String) -> Self {
        WsContext {
            addr,
            cid,
            create_time: now,
        }
    }

    pub fn do_send(&self, msg: &str) {
        self.addr.do_send(SendMessage(String::from(msg)));
    }
}
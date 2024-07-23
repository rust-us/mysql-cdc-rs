use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_http::ws::{CloseCode, CloseReason};
use actix_web_actors::ws;
use crate::web_error::{WebError, WResult};
use crate::wss::event::WSEvent;
use crate::wss::strategy::factory::WSSFactory;

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

    fatory: Option<Rc<WSSFactory>>,
}

impl MyWebSocket {
    pub fn new() -> Self {
        Self {
            hb: Instant::now(),
            fatory: None,
        }
    }

    /// 是否准备就绪
    /// true: 准备就绪
    /// false: 未准备就绪
    pub fn is_ready(&self) -> bool {
        self.fatory.is_some() && self.fatory.as_ref().unwrap().is_ready()
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
}

impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start. We start the heartbeat process here.
    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);
    }
}

/// Handler for ws::Message message: `ws::Message`
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWebSocket {
    /// process websocket messages
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        // // 接收到信息
        // println!("Received WS: {msg:?}");

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
                                ctx.text(resp)
                            }
                        }
                    }
                    Err(err) => {
                        let msg = format!("Cause : {}", err.to_string());
                        println!("{}", &msg);

                        ctx.close(Some(
                            CloseReason::from((CloseCode::Abnormal, msg))
                        ));
                        ctx.stop();
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

                ctx.close(reason);
                ctx.stop();
            },
            _ => (),
            // _ => ctx.stop(),
        }
    }
}

impl MyWebSocket {
    /// 执行策略
    fn exec_action(&mut self, e: &WSEvent) -> WResult<Option<String>> {
        println!(" {:?}", e);

        if self.is_ready() {
            return self.fatory.as_ref().unwrap().strategy(e);
        }

        match e.get_action() {
            0 => {
                if !self.is_ready() {
                    let f = WSSFactory::create();
                    let rc = Rc::new(f);
                    self.fatory = Some(rc);
                }
            },
            _ => {
                //.
            }
        }

        if self.is_ready() {
            return self.fatory.as_ref().unwrap().strategy(e);
        }
        return Err(WebError::Value("Server is not ready".to_string()));
    }
}
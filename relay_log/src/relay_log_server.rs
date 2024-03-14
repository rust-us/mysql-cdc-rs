use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use binlog::events::binlog_event::BinlogEvent;
use common::err::CResult;
use common::err::decode_error::ReError;

use crate::relay_log_server_machine::RelayLogServerMachine;

#[derive(Debug)]
pub struct RelayLogServer {
    state: Arc<Mutex<ServerState>>,
}

#[derive(Debug)]
struct ServerState {
    running: bool,
    receiver_handle: Option<JoinHandle<CResult<()>>>,
}

impl RelayLogServer {
    pub fn new() -> Self {
        let state = Arc::new(Mutex::new(ServerState {
            running: false,
            receiver_handle: None,
        }));
        Self {
            state,
        }
    }

    /// 实例化，且开始监听Receiver
    pub fn new_with_binlog_receiver(rx: Receiver<BinlogEvent>) -> CResult<Self> {
        let state = Arc::new(Mutex::new(ServerState {
            running: false,
            receiver_handle: None,
        }));
        Self::recv_event(Arc::clone(&state), rx)?;
        Ok(Self {
            state
        })
    }

    /// 开启中继日志监听服务（监听上游binlog事件）
    pub fn start(&self, rx: Receiver<BinlogEvent>) -> CResult<bool> {
        Self::recv_event(Arc::clone(&self.state), rx)
    }

    /// 监听binlog事件
    fn recv_event(state: Arc<Mutex<ServerState>>, mut rx: Receiver<BinlogEvent>) -> CResult<bool> {
        let mut s = state.lock().or_else(|e| {
            error!("stare recv binlog event err: {:?}", &e);
            Err(ReError::Error(e.to_string()))
        })?;
        if s.running == true {
            Err(ReError::String("The RelayLogServer is already running.".to_string()))
        } else {
            let shard_state = Arc::clone(&state);
            // 开启一个task监听Receiver
            let handle: JoinHandle<CResult<()>> = tokio::spawn(async move {
                // 监听Receiver。
                // 1. 若通道为空，但是发送端未关闭，则当前task放弃CPU使用权(不会阻塞线程)。
                // 2. 若通道为空，且发送端已经关闭，则收到消息：None
                // 2. 若通道不为空，则接收到消息：Some(event)
                while let Some(event) = rx.recv().await {
                    match RelayLogServerMachine::process_binlog_event(&event) {
                        Ok(()) => {}
                        Err(e) => {
                            error!("Precess BinlogEvent: {:?}, err: {:?}", event, e);
                        }
                    }
                };
                warn!("binlog event sender closed, current receiving end is about to shutdown.");
                let mut end_state = shard_state.lock().or_else(|e| {
                    error!("relay log server close err: {:?}", &e);
                    Err(ReError::Error(e.to_string()))
                })?;
                end_state.running = false;
                end_state.receiver_handle = None;
                Ok(())
            });

            s.running = true;
            s.receiver_handle = Some(handle);
            Ok(true)
        }
    }

    ///
    /// Receiver端异常close()后，只是半关闭状态，Receiver端仍然可以继续读取可能已经缓冲在通道中的消息，
    /// close()只能保证Sender端无法再发送普通的消息，但Permit或OwnedPermit仍然可以向通道发送消息。
    /// 只有通道已空且所有Sender端(包括Permit和OwnedPermit)都已经关闭的情况下，recv()才会返回None，此时代表通道完全关闭。
    fn try_recv_event(&self, mut rx: Receiver<BinlogEvent>) {
        loop {
            match rx.try_recv() {
                Ok(event) => {
                    info!("收到binlog事件：{:?}", event);
                }
                Err(e) => {
                    warn!("binlog event Receiver try_recv: {:?}", e);
                    break;
                }
            }
        }
    }

    /// 关闭中继日志监听服务。
    /// 不主动关闭binlog监听端服务，receiver状态完全由sender决定。
    /// 只要存在一个sender未关闭，RelayLogServer则一直运行。
    // pub fn shutdown(&mut self) -> CResult<bool> {
    //     let state = self.state.lock();
    //     match state {
    //         Ok(mut s) => {
    //             if let Some(handle) = s.receiver_handle.take() {
    //                 handle.abort();
    //             }
    //             s.running = false;
    //             Ok(true)
    //         }
    //         Err(e) => {
    //             Err(ReError::Error(e.to_string()))
    //         }
    //     }
    // }

    pub fn is_running(&self) -> CResult<bool> {
        return match self.state.lock() {
            Ok(s) => {
                Ok(s.running)
            }
            Err(e) => {
                Err(ReError::String(e.to_string()))
            }
        };
    }
}
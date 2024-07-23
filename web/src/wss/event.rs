use serde_json::{from_str, Error};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::web_error::WResult;

#[derive(Debug, Serialize, Deserialize)]
pub struct WSEvent {
    action: i16,
    body: HashMap<String, String>,

    /// 是否为空
    empty: Option<bool>,
}

impl WSEvent {
    pub fn new(action: i16, body: HashMap<String, String>) -> Self {
        let is_empty = body.is_empty();

        WSEvent {
            action,
            body,
            empty: Some(is_empty),
        }
    }

    pub fn parser(msg: String) -> WResult<WSEvent> {
        if msg.is_empty() {
            return Ok(WSEvent::default());
        }

        let c:Result<Self, Error> = from_str(&msg);

        return match c {
            Ok(event) => {
                // println!("Action: {}", event.action);
                // for (key, value) in &event.body {
                //     println!("{}: {}", key, value);
                // }

                Ok(event)
            },
            Err(e) => {
                Err(e.into())
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        return match self.empty {
            None => {
                true
            }
            Some(r) => {
                r
            }
        }
    }

    pub fn get_action(&self) -> i16 {
        self.action
    }

    pub fn get_body(&self) -> HashMap<String, String> {
        self.body.clone()
    }
}

impl Default for WSEvent {
    fn default() -> Self {
        WSEvent {
            action: -1,
            body: HashMap::default(),
            empty: Some(true),
        }
    }
}
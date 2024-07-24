use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use lazy_static::lazy_static;
use crate::wss::server::WsContext;

// 使用 Mutex/RwLock 来确保 HashMap 的线程安全
lazy_static! {
    static ref WS: RwLock<HashMap<String, Arc<WsContext>>> = RwLock::new(HashMap::new());
}

pub struct SessionManager {

}

impl SessionManager {

    /// 添加元素
    pub fn ws_insertupdate(key: String, value: WsContext) {
        // 注意：在 `map` 锁定期间，你不能再次锁定它，直到锁被释放
        let mut map = WS.write().unwrap();
        map.insert(key, Arc::new(value));
    }

    /// 读取元素
    pub fn ws_get(key: &str) -> Option<Arc<WsContext>> {
        let guard = WS.read().unwrap();

        return match guard.get(key) {
            None => {
                None
            }
            Some(v) => {
                Some(v.clone())
            }
        }
    }

    pub fn ws_remove(key: &str) -> Option<Arc<WsContext>> {
        let mut map = WS.write().unwrap();

        map.remove(key)
    }
}
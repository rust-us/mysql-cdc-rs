use std::collections::HashMap;
use crate::binlog::binlog_events_wrapper::{BinlogEventsHolderRef};

// 定义一个函数签名，用于注册的函数
pub type Callback = fn(BinlogEventsHolderRef, usize) -> bool;

// 定义函数注册表结构
pub struct FunctionRegistry {
    callbacks: HashMap<&'static str, Callback>,
}

impl FunctionRegistry {
    /// 创建一个新的函数注册表
    pub fn new() -> Self {
        Self {
            callbacks: HashMap::new(),
        }
    }

    /// 注册函数
    pub fn register_function(&mut self, name: &'static str, callback: fn(BinlogEventsHolderRef, usize) -> bool) {
        self.callbacks.insert(name, callback);
    }

    /// 调用已注册的函数
    pub fn call_function(&self, name: &'static str, arg: BinlogEventsHolderRef, step: usize) -> Option<bool> {
        if let Some(callback) = self.callbacks.get(name) {
            Some(callback(arg, step))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    pub type CallbackDemo = fn(EventHandlerRef, usize) -> bool;

    pub struct FunctionRegistryDemo {
        callbacks: HashMap<&'static str, CallbackDemo>,
    }

    impl FunctionRegistryDemo {
        pub fn new() -> Self {
            Self {
                callbacks: HashMap::new(),
            }
        }

        pub fn register_function(&mut self, name: &'static str, callback: fn(EventHandlerRef, usize) -> bool) {
            self.callbacks.insert(name, callback);
        }

        pub fn call_function(&self, name: &'static str, arg: EventHandlerRef, step: usize) -> Option<bool> {
            if let Some(callback) = self.callbacks.get(name) {
                Some(callback(arg, step))
            } else {
                None
            }
        }
    }

    pub type EventHandlerRef = Arc<RefCell<EventHandler>>;

    #[derive(Debug)]
    pub struct EventHandler {
        // 开始时间
        start_time: Instant,

        // 耗时
        during_time: Option<Duration>,
    }

    impl EventHandler {
        pub fn new() -> Self {
            EventHandler {
                start_time: Instant::now(),
                during_time: None,
            }
        }


        fn end_of_elapsed_time(&mut self) {
            let end_time = Instant::now();

            let elapsed_time = end_time - self.start_time;
            self.during_time = Some(elapsed_time);
        }
    }


    /// 事件函数
    fn end_of_elapsed_time(wrapper: EventHandlerRef, x: usize) -> bool {
        wrapper.borrow_mut().end_of_elapsed_time();

        true
    }
    
    // 示例函数1
    fn add_one(a: EventHandlerRef, x: usize) -> bool {
        a.borrow_mut().end_of_elapsed_time();

        true
    }

    // 示例函数2
    fn multiply_by_two(a: EventHandlerRef, x: usize) -> bool {
        true
    }

    #[test]
    fn test() {
        let wrapper = EventHandler::new();
        let wrapper = Arc::new(RefCell::new(wrapper));

        // 创建函数注册表
        let mut registry = FunctionRegistryDemo::new();

        // 注册函数
        registry.register_function("add_one", add_one);
        registry.register_function("multiply_by_two", multiply_by_two);

        // 调用已注册的函数
        if let Some(result) = registry.call_function("add_one", wrapper.clone(), 5) {
            println!("Result of add_one: {}", result);
        } else {
            println!("Function not found");
        }

        if let Some(result) = registry.call_function("multiply_by_two", wrapper.clone(), 3) {
            println!("Result of multiply_by_two: {}", result);
        } else {
            println!("Function not found");
        }
    }
}
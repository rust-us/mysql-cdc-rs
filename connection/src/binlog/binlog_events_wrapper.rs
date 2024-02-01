use std::cell::{RefCell};
use std::sync::Arc;
use std::time::{Duration, Instant};
use binlog::events::binlog_event::BinlogEvent;
use common::err::CResult;
use crate::binlog::binlog_events::BinlogEvents;
use crate::binlog::reg::FunctionRegistry;

const END_OF_ELAPSED_TIME :&str = "end_of_elapsed_time";
const UPDATE_RECEIVES_BYTES :&str = "update_receives_bytes";

pub type BinlogEventsHolderRef = Arc<RefCell<BinlogEventsHolder>>;

#[derive(Debug)]
pub struct BinlogEventsWrapper {
    wrapper: BinlogEventsHolderRef,

}

pub struct BinlogEventsWrapperIter {
    index: usize,

    binlogs: Arc<RefCell<BinlogEvents>>,

    wrapper: BinlogEventsHolderRef,

    registry: FunctionRegistry,
}

#[derive(Debug)]
pub struct BinlogEventsHolder {
    binlogs: Arc<RefCell<BinlogEvents>>,

    // 开始时间
    start_time: Instant,

    // 耗时
    during_time: Option<Duration>,

    // assert_eq!(usize::MIN, 0)
    // assert_eq!(usize::MAX, 18446744073709551615)
    receives_bytes_len: usize,
}

impl BinlogEventsWrapper {
    pub fn new(binlogs: Arc<RefCell<BinlogEvents>>) -> Self {
        let wrapper = BinlogEventsHolder::new(binlogs);

        BinlogEventsWrapper {
            wrapper: Arc::new(RefCell::new(wrapper)),
        }
    }

    pub fn get_iter(&self) -> BinlogEventsWrapperIter {
        let iter = BinlogEventsWrapperIter::new(self.wrapper.borrow().binlogs.clone(), self.wrapper.clone());

        iter
    }

    pub fn get_during_time(&self) -> Option<Duration> {
        self.wrapper.borrow_mut().get_during_time()
    }

    pub fn get_receives_bytes_len(&self) -> usize {
        self.wrapper.borrow_mut().get_receives_bytes_len()
    }

}

impl BinlogEventsHolder {
    fn new(binlogs: Arc<RefCell<BinlogEvents>>) -> BinlogEventsHolder {
        BinlogEventsHolder {
            binlogs,
            start_time: Instant::now(),
            during_time: None,
            receives_bytes_len: 0,
        }
    }

    fn get_during_time(&self) -> Option<Duration> {
        self.during_time.clone()
    }

    fn update_end_of_elapsed_time(&mut self) {
        let end_time = Instant::now();

        let elapsed_time = end_time - self.start_time;
        self.during_time = Some(elapsed_time);
    }

    fn update_receives_bytes(&mut self, len: i32) {
        self.receives_bytes_len += len as usize;
    }

    fn get_receives_bytes_len(&self) -> usize {
        self.receives_bytes_len
    }
}

/// 事件函数
fn end_of_elapsed_time(wrapper: BinlogEventsHolderRef, x: i32) -> bool {
    wrapper.borrow_mut().update_end_of_elapsed_time();

    true
}

fn update_receives_bytes(wrapper: BinlogEventsHolderRef, len: i32) -> bool {
    wrapper.borrow_mut().update_receives_bytes(len);

    true
}

impl BinlogEventsWrapperIter {
    fn new(binlogs: Arc<RefCell<BinlogEvents>>, wrapper: BinlogEventsHolderRef) -> Self {
        let mut registry = FunctionRegistry::new();

        // 注册函数
        registry.register_function(END_OF_ELAPSED_TIME, end_of_elapsed_time);
        registry.register_function(UPDATE_RECEIVES_BYTES, update_receives_bytes);

        BinlogEventsWrapperIter {
            index: 0,
            binlogs,
            wrapper,
            registry,
        }
    }
}

impl Iterator for BinlogEventsWrapperIter {
    type Item = CResult<Vec<BinlogEvent>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.binlogs.borrow_mut().next();
        if next.is_none()  {
            self.registry.call_function(END_OF_ELAPSED_TIME, self.wrapper.clone(), 1);

            return None;
        }

        let events = next.unwrap();

        // update bytes len， 同步操作会带来 12% 的性能损耗
        let len = match &events {
            Ok(list) => {
                list.iter().map(|x| x.len()).sum()
            }
            Err(err) => {
                0
            }
        };
        self.registry.call_function(UPDATE_RECEIVES_BYTES, self.wrapper.clone(), len);

        Some(events)
    }
}
use std::cell::{RefCell};
use std::sync::Arc;
use std::time::{Duration, Instant};
use binlog::events::binlog_event::BinlogEvent;
use common::err::CResult;
use crate::binlog::binlog_events::BinlogEvents;
use crate::binlog::reg::FunctionRegistry;

const END_OF_ELAPSED_TIME :&str = "end_of_elapsed_time";
const ADD_RECEIVES_BYTES:&str = "add_receives_bytes";

// pub type BinlogEventsWrapperRef = Arc<RefCell<BinlogEventsWrapper>>;
pub type BinlogEventsHolderRef = Arc<RefCell<BinlogEventsHolder>>;

#[derive(Debug)]
pub struct BinlogEventsWrapper {
    holder: BinlogEventsHolderRef,

}

#[derive(Debug)]
pub struct BinlogEventsHolder {
    binlogs: Arc<RefCell<BinlogEvents>>,

    // 开始时间
    start_time: Instant,

    // 耗时
    during_time: Option<Duration>,
}

pub struct BinlogEventsWrapperIter {
    index: usize,

    binlogs: Arc<RefCell<BinlogEvents>>,

    holder: BinlogEventsHolderRef,

    registry: FunctionRegistry,
}

impl BinlogEventsWrapper {
    pub fn new(binlogs: Arc<RefCell<BinlogEvents>>) -> Self {
        let wrapper = BinlogEventsHolder::new(binlogs);

        BinlogEventsWrapper {
            holder: Arc::new(RefCell::new(wrapper)),
        }
    }

    pub fn get_iter(&self) -> BinlogEventsWrapperIter {
        let iter = BinlogEventsWrapperIter::new(self.holder.borrow().binlogs.clone(), self.holder.clone());

        iter
    }

    pub fn get_during_time(&self) -> Option<Duration> {
        self.holder.borrow().get_during_time()
    }

    /// 获取接受到的流量总大小
    pub fn get_receives_bytes(&self) -> usize {
        self.holder.borrow().get_receives_bytes()
    }
}

impl BinlogEventsHolder {
    fn new(binlogs: Arc<RefCell<BinlogEvents>>) -> BinlogEventsHolder {
        BinlogEventsHolder {
            binlogs,
            start_time: Instant::now(),
            during_time: None,
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

    /// 获取接受到的流量总大小
    fn get_receives_bytes(&self) -> usize {
        self.binlogs.borrow().get_receives_bytes()
    }
}

/// 事件函数
fn end_of_elapsed_time(mut wrapper: BinlogEventsHolderRef, x: usize) -> bool {
    wrapper.borrow_mut().update_end_of_elapsed_time();

    true
}

impl BinlogEventsWrapperIter {
    fn new(binlogs: Arc<RefCell<BinlogEvents>>, wrapper: BinlogEventsHolderRef) -> Self {
        let mut registry = FunctionRegistry::new();

        // 注册函数
        registry.register_function(END_OF_ELAPSED_TIME, end_of_elapsed_time);
        // registry.register_function(ADD_RECEIVES_BYTES, add_receives_bytes);

        BinlogEventsWrapperIter {
            index: 0,
            binlogs,
            holder: wrapper,
            registry,
        }
    }

    /// 获取接受到的流量总大小
    fn get_receives_bytes(&self) -> usize {
        self.binlogs.borrow().get_receives_bytes()
    }
}

impl Iterator for BinlogEventsWrapperIter {
    type Item = CResult<Vec<BinlogEvent>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.binlogs.borrow_mut().next();
        if next.is_none()  {
            self.registry.call_function(END_OF_ELAPSED_TIME, self.holder.clone(), 1);

            return None;
        }

        let events = next.unwrap();

        // // update bytes len， 同步操作会带来 12% 的性能损耗
        // let len = match &events {
        //     Ok(list) => {
        //         list.iter().map(|x| x.len()).sum()
        //     }
        //     Err(err) => {
        //         error!("BinlogEventsWrapperIter next error:{:?}", &err);
        //
        //         0
        //     }
        // };
        // self.registry.call_function(ADD_RECEIVES_BYTES, self.wrapper.clone(), len as usize);
        //
        // // check
        // let a = self.get_receives_bytes();
        // let b = self.binlogs.borrow().get_receives_bytes();

        Some(events)
    }
}
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use common::structure::queue::{Queue};
use ringbuffer::{AllocRingBuffer, RingBuffer};
use serde::Serialize;
use tracing::debug;
use common::err::CResult;

pub const DEFAULT_MAX_ALLOC_RING_BUFFER_LEN: usize = 5120;

/// 队列管理器。
pub struct QueueManager<T> {
    queue_maps: HashMap<u64, Arc<RwLock<QueueRingBuffer<T>>>>,
    
}

/// 队列管理器配置
pub struct QueueManagerOptions {
    queue_default_capacity: usize,

}

/// Singleton using Mutex, which allows declaring a static variable with lazy initialization at first access.
#[derive(Debug)]
pub struct QueueRingBuffer<T> {
    queue: AllocRingBuffer<T>,
}


/// Singleton using Mutex, which allows declaring a static variable with lazy initialization at first access.
#[derive(Debug)]
pub struct QueueRingBufferBlocked {
}

pub trait Wake: Clone {
    fn wake(&self);
}

#[derive(Clone)]
struct WakeInstance {
    inner: std::thread::Thread,
}

impl WakeInstance {
    pub fn new(thread: std::thread::Thread) -> Self {
        Self {
            inner: thread
        }
    }
}

impl Wake for WakeInstance {
    fn wake(&self) {
        debug!("wake instance call wake, unpark thread.");
        self.inner.unpark();        // 唤醒线程
    }
}

// 从一个Wake实例中产生RawWaker，继而产生Waker
fn create_raw_waker<W: Wake>(wake: W) -> RawWaker {
    debug!("create a raw waker.");
    RawWaker::new(
        Box::into_raw(Box::new(wake)) as *const(),
        &RawWakerVTable::new(
            |data| unsafe {
                debug!("raw waker vtable clone");
                create_raw_waker((&*(data as *const W)).clone())    // 把data克隆一份(要求泛型W必须实现Clone Trait)，重新生成RawWaker
            },
            |data| unsafe {
                debug!("raw waker vtable wake");
                Box::from_raw(data as *mut W).wake()        // data就是wake实例， 调用wake实例的wake方法唤醒线程
            },
            |data| unsafe {
                debug!("raw waker vtable wake_by_ref");
                (&*(data as *const W)).wake()
            },
            |data| unsafe {
                debug!("raw waker vtable drop");
                drop(Box::from_raw(data as *mut W))
            }
        )
    )
}

impl QueueRingBufferBlocked {
    pub fn block_on<F: Future>(future: F) -> F::Output {
        // convert self to Pin<&mut Self>. 因为poll(self: Pin<&mut Self>, cx: &mut Context<'_>) ，所以必须将future钉住
        pin_utils::pin_mut!(future);

        // 定义一个waker，如果future为未就绪的话，需要waker去唤醒
        // 不同的Executor有不同的waker实现，这里需要自定义waker,在本block_on的实现中,waker自然就是唤醒当前线程即可
        // 不同的waker实现有同一的接口实现，需要通过自定义虚函数实现，这里自己实现这一部分。
        let thread = std::thread::current();
        let wake_instance = WakeInstance::new(thread);

        let raw_waker = create_raw_waker(wake_instance);
        let waker = unsafe { Waker::from_raw(raw_waker) };
        let mut cx = Context::from_waker(&waker);

        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(t) => {
                    debug!("future is ready, return is final result.");
                    return t;
                },
                Poll::Pending => {
                    debug!("future is not ready, register waker, wait util ready.");
                    std::thread::park();
                }
            }
        }
    }
}

impl <T> QueueManager<T> {
    pub fn new() -> Self {
        QueueManager {
            queue_maps: HashMap::<u64, Arc<RwLock<QueueRingBuffer<T>>>>::new(),
        }
    }

    pub fn get_queue(&mut self, binlog_reader_id:u64, or_default_option: QueueManagerOptions) -> Option<Arc<RwLock<QueueRingBuffer<T>>>> {
        if !self.queue_maps.contains_key(&binlog_reader_id) {
            let buffer = QueueRingBuffer::<T>::new_with_capacity(or_default_option.get_queue_default_capacity());

            self.queue_maps.insert(binlog_reader_id, Arc::new(RwLock::new(buffer)));
        }

        let ring_buffer_maybe = self.queue_maps.get(&binlog_reader_id);
        if let Some(mut ring_buffer_arc) = ring_buffer_maybe {
            return Some(ring_buffer_arc.clone());
        }

        None
    }
}

impl Default for QueueManagerOptions {
    fn default() -> Self {
        QueueManagerOptions::new(DEFAULT_MAX_ALLOC_RING_BUFFER_LEN)
    }
}

impl QueueManagerOptions {
    pub fn new(queue_default_capacity: usize) -> Self {
        QueueManagerOptions {
            queue_default_capacity,
        }
    }

    pub fn get_queue_default_capacity(&self) -> usize {
        self.queue_default_capacity
    }
}

impl<T> Default for QueueRingBuffer<T> {
    fn default() -> Self {
        QueueRingBuffer::new_with_capacity(DEFAULT_MAX_ALLOC_RING_BUFFER_LEN)
    }
}

impl<T> QueueRingBuffer<T> {
    pub fn new(buffer: AllocRingBuffer<T>) -> Self {
        QueueRingBuffer {
            queue: buffer,
        }
    }

    pub fn new_with_capacity(capacity: usize) -> Self {
        let buffer: AllocRingBuffer<T> = AllocRingBuffer::<T>::new(capacity);

        QueueRingBuffer::new(buffer)
    }

    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

/// async
impl<T> QueueRingBuffer<T> {
    pub fn push(&mut self, log: T) -> CResult<bool> {
        if self.is_full() {
            return Ok(false);
        }

        self.queue.push(log);

        Ok(true)
    }

    pub fn pop(&mut self) -> Option<T> {
        // the top item off the ringbuffer, and moves this item out.
        self.queue.dequeue()
    }
}

#[cfg(test)]
mod test {
    use tracing::debug;
    use common::log::tracing_factory::TracingFactory;
    use common::structure::queue::{Queue};
    use crate::events::binlog_event::BinlogEvent;
    use crate::events::event_header::Header;
    use crate::events::protocol::xid_event::XidLogEvent;
    use crate::relay_log::queue_manager::{QueueManager, QueueManagerOptions, QueueRingBufferBlocked};

    #[test]
    fn test_cap() {

        TracingFactory::init_log(true);
        debug!("QueueManager");

        // new
        let mut manager = QueueManager::<BinlogEvent>::new();

        let queue_may = manager.get_queue(1, QueueManagerOptions::new(8));
        assert!(queue_may.is_some());

        let queue_ref = queue_may.unwrap();

        // push
        {
            let mut queue = queue_ref.write().expect("queue_ref write 失败");

            let mut total_count = 0;
            for i in 0..6 {
                let e = BinlogEvent::XID(XidLogEvent::new(Header::default(), i));
                let rs = queue.push(e);
                total_count += 1;

                assert!(rs.is_ok());
                assert!(rs.unwrap());
                assert_eq!(queue.len(), total_count);
            }
            for i in 0..6 {
                let e = BinlogEvent::XID(XidLogEvent::new(Header::default(), i));
                let rs = queue.push(e);
                total_count += 1;

                assert!(rs.is_ok());

                if total_count <= 8 {
                    assert!(rs.unwrap());
                } else {
                    assert!(!rs.unwrap());
                }
            }
            for i in 0..8 {
                let e = BinlogEvent::XID(XidLogEvent::new(Header::default(), i));
                let rs = queue.push(e);
                total_count += 1;

                assert!(rs.is_ok());
                assert!(!rs.unwrap());
            }
        }

        // pop
        let event = queue_ref.write().expect("queue_ref write 失败").pop();
        assert!(event.is_some());
        assert_eq!(queue_ref.write().expect("queue_ref write 失败").len(), 7);

        let event = queue_ref.write().expect("queue_ref write 失败").pop();
        assert!(event.is_some());
        assert_eq!(queue_ref.write().expect("queue_ref write 失败").len(), 6);

        let event = queue_ref.write().expect("queue_ref write 失败").pop();
        assert!(event.is_some());
        assert_eq!(queue_ref.write().expect("queue_ref write 失败").len(), 5);
    }

    #[test]
    fn test_new_and_pop() {

        // new
        let mut manager = QueueManager::<BinlogEvent>::new();

        let queue_may = manager.get_queue(1, QueueManagerOptions::default());
        assert!(queue_may.is_some());

        let queue_ref = queue_may.unwrap();

        // push
        let rs = queue_ref.write().expect("queue_ref write 失败").push(BinlogEvent::XID(XidLogEvent::new(Header::default(), 1)));
        assert!(rs.is_ok());
        let rs = queue_ref.write().expect("queue_ref write 失败").push(BinlogEvent::XID(XidLogEvent::new(Header::default(), 2)));
        assert!(rs.is_ok());
        assert_eq!(queue_ref.write().expect("queue_ref write 失败").len(), 2);

        let mut queue = queue_ref.write().expect("queue_ref write 失败");
        // pop
        let event = queue.pop();
        assert!(event.is_some());
        // assert_eq!(event.unwrap().get_inner().capacity(), 5);
        assert_eq!(queue.len(), 1);

        let event = queue.pop();
        assert!(event.is_some());
        // assert_eq!(event.unwrap().get_inner().capacity(), 5);
        assert_eq!(queue.len(), 0);

        let event = queue.pop();
        assert!(event.is_none());
        assert_eq!(queue.len(), 0);
    }
}
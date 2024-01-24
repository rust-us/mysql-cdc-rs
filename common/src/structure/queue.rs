use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, RwLock};
use tracing::instrument;

pub trait Queue<T> {
    /// 定义一个新的Queue实例，包含了内部数据的Arc<Mutex>结构。
    fn new() -> Self;

    /// 向Queue中添加元素， 元素入队
    fn push(&self, val: T);

    /// 从Queue中弹出元素， 元素出队
    fn pop(&self) -> Option<T>;

    /// 查看队首元素， 确认队列的头部元素。 不转移所有权。
    fn peek(&self) -> Option<T>;

    /// 是否为空
    fn is_empty(&self) -> bool;

    /// 队列的大小
    fn len(&self) -> usize;

}

#[derive(Debug, Clone)]
/// Queue结构体
pub struct QueueImpl<T: Clone + Debug> {
    inner: Arc<Mutex<VecDeque<T>>>,
}

unsafe impl <T: Clone + Debug> Send for QueueImpl<T>{}
unsafe impl <T: Clone + Debug> Sync for QueueImpl<T>{}

impl<T: Clone + Debug> Queue<T> for QueueImpl<T> {
    #[inline]
    fn new() -> Self where Self: Sized {
        QueueImpl {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    #[inline]
    #[instrument]
    fn push(&self, val: T) {
        // 通过使用Mutex来获取inner数据的可变引用，
        // 使用mutex实例的lock方法加锁（获得锁），在获取到锁之后，就可以安全地对inner进行修改。如果不能获取到锁，则会阻塞当前线程，直到获取到锁。
        let mut inner = self.inner.lock().unwrap();
        inner.push_back(val);
    }

    #[inline]
    #[instrument]
    fn pop(&self) -> Option<T> {
        let mut inner = self.inner.lock().unwrap();
        match inner.len() {
            // 如果队列当前为空，则返回None。
            0 => None,
            // 否则，从inner中弹出并返回队列头部的第一个元素
            _ => inner.pop_front(),
        }
    }

    #[inline]
    fn peek(&self) -> Option<T> {
        let inner = self.inner.lock().unwrap();
        match inner.len() {
            0 => None,
            // _ => QueueItemGuard {
            //     inner: Box::new(&self),
            //     item: inner.front(),
            // },
            _ => Some(inner[0].clone()),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.is_empty()
    }

    #[inline]
    fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::structure::queue::*;

    #[test]
    fn test_queue() {
        let queue: QueueImpl<i32> = QueueImpl::new();
        assert_eq!(queue.is_empty(), true);

        queue.push(10);
        queue.push(11);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.peek(), Some(10));
        assert_eq!(queue.pop(), Some(10));
        assert_eq!(queue.pop(), Some(11));
        assert_eq!(queue.pop(), None);
        assert_eq!(queue.is_empty(), true);

        queue.push(20);
        assert_eq!(queue.is_empty(), false);
    }

    #[test]
    fn test_pop() {
        let mut queue: QueueImpl<i32> = QueueImpl::new();
        for i in 0..100 {
            queue.push(i);
        }

        while let Some(v) = queue.pop() {
            print!("{v}, ");
        }
    }
}
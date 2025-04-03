use super::ThreadPool;

/// 尚未实现
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::Result<Self> where Self: Sized {
        todo!()
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        todo!()
    }
}
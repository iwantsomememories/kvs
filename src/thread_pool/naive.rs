use super::ThreadPool;
use crate::Result;

use std::thread;

/// 简易线程"池"实现（实际不复用线程）
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> where Self: Sized {
        Ok(NaiveThreadPool) // 忽略线程数参数
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        thread::spawn(job); // 直接创建新线程
    }
}
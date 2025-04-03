//! 该模块提供各类线程池

use crate::Result;


/// 线程池抽象接口
/// 
/// 该trait定义了线程池的基本行为规范，
/// 不同的具体实现可以提供各自的线程调度策略
pub trait ThreadPool {
    /// 创建新的线程池实例
    ///
    /// # 参数
    /// * `threads`: 线程池中初始线程数量
    ///
    /// # 返回
    /// * `Result<Self>`: 成功时返回线程池实例，失败时返回错误
    ///
    /// # 注意
    /// 1. 实现应立即创建指定数量的工作线程
    /// 2. 任一线程创建失败时应终止所有已创建线程
    /// 3. 错误类型由具体实现定义
    fn new(threads: u32) -> Result<Self> where Self: Sized;

    /// 向线程池提交任务
    ///
    /// # 参数
    /// * `job`: 要执行的任务闭包
    ///
    /// # 泛型约束
    /// * `F`: 满足以下特征的闭包类型：
    ///   - `FnOnce()`: 可调用一次的闭包
    ///   - `Send`: 可跨线程安全传递
    ///   - `'static`: 不包含非静态引用
    ///
    /// # 保证
    /// 1. 任务提交必定成功（不返回错误）
    /// 2. 即使任务执行时panic也不会影响线程池运行
    /// 3. 线程池会维持固定的线程数量
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

mod naive;
mod rayon;
mod shared_queue;

pub use naive::NaiveThreadPool;
pub use rayon::RayonThreadPool;
pub use shared_queue::SharedQueueThreadPool;
use std::thread;

use crossbeam_channel::{unbounded, Receiver, Sender};
use super::ThreadPool;
use crate::{Result, KvsError};

type Job = Box<dyn FnOnce() + Send + 'static>;

enum ThreadPoolMessage {
    RunJob(Job),
    Shutdown,
}

#[derive(Clone)]
struct TaskReceiver(Receiver<ThreadPoolMessage>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_tasks(rx)) {
                eprintln!("Failed to spawn a thread: {}", e);
            }
        }
    }
}

fn run_tasks(rx: TaskReceiver) {
    loop {
        match rx.0.recv() {
            Ok(ThreadPoolMessage::RunJob(task)) => {
                task();
            }
            Err(_) | Ok(ThreadPoolMessage::Shutdown) => break,
        }
    }
}

/// 通过共享队列实现的线程池
/// 
/// 如果线程池中的一个线程发生panic，那么旧线程会被销毁，同时创建一个新线程。
pub struct SharedQueueThreadPool {
    task_sender: Sender<ThreadPoolMessage>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        if threads <= 0 {
            return Err(KvsError::StringError("Argument 'threads' must be positive".to_string()));
        }

        let (s, r) = unbounded();

        for _ in 0..threads {
            let task_receiver = TaskReceiver(r.clone());
            thread::Builder::new().spawn(move || run_tasks(task_receiver))?;
        }

        Ok(Self { task_sender: s })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let message = ThreadPoolMessage::RunJob(Box::new(job));

        self.task_sender.send(message).expect("The thread pool has no thread.");
    }
}
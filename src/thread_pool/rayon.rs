use super::ThreadPool;
use crate::{Result, KvsError};

use rayon::prelude::*;

/// rayon::ThreadPool的包装
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        if threads <= 0 {
            return Err(KvsError::StringError("Argument 'threads' must be positive".to_string()));
        }

        let pool = rayon::ThreadPoolBuilder::new().num_threads(threads as usize).build()?;

        Ok(RayonThreadPool(pool))
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.0.spawn(job);
    }
}
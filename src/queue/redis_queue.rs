use std::time::Duration;

use deadpool_redis::{Config, Pool, PoolConfig, Runtime};
use redis::AsyncCommands;
use uuid::Uuid;

use crate::error::{AppError, AppResult};

#[derive(Clone)]
pub struct JobQueue {
    pool: Pool,
    queue_key: String,
}

impl JobQueue {
    pub fn new(pool: Pool, queue_key: String) -> Self {
        Self { pool, queue_key }
    }

    pub async fn push(&self, id: Uuid) -> AppResult<()> {
        let mut conn = self.pool.get().await?;
        let _: () = conn.lpush(&self.queue_key, id.to_string()).await?;
        Ok(())
    }

    pub async fn pop_blocking(&self, timeout: Duration) -> AppResult<Option<Uuid>> {
        let mut conn = self.pool.get().await?;
        let result: Option<(String, String)> = conn
            .brpop(&self.queue_key, timeout.as_secs_f64())
            .await?;
        match result {
            Some((_, id)) => Uuid::parse_str(&id)
                .map(Some)
                .map_err(|e| AppError::Internal(format!("invalid id in queue: {e}"))),
            None => Ok(None),
        }
    }

    pub async fn depth(&self) -> AppResult<u64> {
        let mut conn = self.pool.get().await?;
        let n: u64 = conn.llen(&self.queue_key).await?;
        Ok(n)
    }
}

pub fn build_pool(url: &str, size: usize) -> AppResult<Pool> {
    let mut cfg = Config::from_url(url);
    cfg.pool = Some(PoolConfig::new(size));
    cfg.create_pool(Some(Runtime::Tokio1))
        .map_err(|e| AppError::Internal(format!("redis pool: {e}")))
}

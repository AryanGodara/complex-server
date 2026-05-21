use std::time::Duration;

use deadpool_redis::Pool;
use redis::AsyncCommands;
use uuid::Uuid;

use crate::domain::job::Job;
use crate::error::AppResult;

#[derive(Clone)]
pub struct ResultCache {
    pool: Pool,
    prefix: String,
    ttl: Duration,
}

impl ResultCache {
    pub fn new(pool: Pool, prefix: String, ttl_seconds: u64) -> Self {
        Self {
            pool,
            prefix,
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    fn key(&self, id: Uuid) -> String {
        format!("{}{}", self.prefix, id)
    }

    pub async fn put(&self, job: &Job) -> AppResult<()> {
        let mut conn = self.pool.get().await?;
        let payload = serde_json::to_string(job)?;
        let _: () = conn
            .set_ex(self.key(job.id), payload, self.ttl.as_secs())
            .await?;
        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> AppResult<Option<Job>> {
        let mut conn = self.pool.get().await?;
        let raw: Option<String> = conn.get(self.key(id)).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }
}

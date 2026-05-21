use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::domain::calculation::{Calculation, CalculationResult};
use crate::domain::job::{Job, JobStatus};
use crate::error::{AppError, AppResult};

#[derive(Clone)]
pub struct JobLedger {
    pool: SqlitePool,
}

impl JobLedger {
    pub async fn connect(database_url: &str) -> AppResult<Self> {
        let opts = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| AppError::Internal(format!("invalid sqlite url: {e}")))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await.map_err(|e| {
            AppError::Internal(format!("migration failed: {e}"))
        })?;

        Ok(Self { pool })
    }

    pub async fn insert(&self, job: &Job) -> AppResult<()> {
        let payload = serde_json::to_string(&job.calculation)?;
        sqlx::query(
            "INSERT INTO jobs (id, kind, payload, status, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(job.id.to_string())
        .bind(job.calculation.kind())
        .bind(payload)
        .bind(job.status.as_str())
        .bind(job.created_at.timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_running(&self, id: Uuid) -> AppResult<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE jobs SET status = 'running', started_at = ?2 \
             WHERE id = ?1 AND status = 'queued'",
        )
        .bind(id.to_string())
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_completed(
        &self,
        id: Uuid,
        result: &CalculationResult,
    ) -> AppResult<()> {
        let now = Utc::now().timestamp_millis();
        let result_json = serde_json::to_string(result)?;
        sqlx::query(
            "UPDATE jobs SET status = 'completed', result = ?2, completed_at = ?3 \
             WHERE id = ?1",
        )
        .bind(id.to_string())
        .bind(result_json)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, id: Uuid, error: &str) -> AppResult<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE jobs SET status = 'failed', error = ?2, completed_at = ?3 \
             WHERE id = ?1",
        )
        .bind(id.to_string())
        .bind(error)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> AppResult<Option<Job>> {
        let row = sqlx::query(
            "SELECT id, payload, status, result, error, \
                    created_at, started_at, completed_at \
             FROM jobs WHERE id = ?1",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let id_str: String = row.try_get("id")?;
        let payload: String = row.try_get("payload")?;
        let status_str: String = row.try_get("status")?;
        let result_str: Option<String> = row.try_get("result")?;
        let error: Option<String> = row.try_get("error")?;
        let created_at: i64 = row.try_get("created_at")?;
        let started_at: Option<i64> = row.try_get("started_at")?;
        let completed_at: Option<i64> = row.try_get("completed_at")?;

        let calculation: Calculation = serde_json::from_str(&payload)?;
        let result = match result_str {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        };

        Ok(Some(Job {
            id: Uuid::parse_str(&id_str)
                .map_err(|e| AppError::Internal(format!("bad uuid in db: {e}")))?,
            calculation,
            status: JobStatus::from_str(&status_str)
                .ok_or_else(|| AppError::Internal(format!("bad status: {status_str}")))?,
            created_at: millis_to_utc(created_at)?,
            started_at: started_at.map(millis_to_utc).transpose()?,
            completed_at: completed_at.map(millis_to_utc).transpose()?,
            result,
            error,
        }))
    }
}

fn millis_to_utc(ms: i64) -> AppResult<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| AppError::Internal(format!("bad timestamp: {ms}")))
}

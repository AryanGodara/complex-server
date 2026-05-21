use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use uuid::Uuid;

use crate::domain::job::{Job, JobStatus};
use crate::error::{AppError, AppResult};
use crate::http::dto::{HealthResponse, SubmitRequest, SubmitResponse, WaitParams};
use crate::state::AppState;

const MAX_WAIT_MS: u64 = 60_000;

pub async fn health(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let depth = state.queue.depth().await.unwrap_or(0);
    Ok(Json(HealthResponse {
        status: "ok",
        queue_depth: depth,
    }))
}

pub async fn submit(
    State(state): State<AppState>,
    Json(body): Json<SubmitRequest>,
) -> AppResult<impl IntoResponse> {
    let job = Job::new(body.calculation);
    state.ledger.insert(&job).await?;
    state.queue.push(job.id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitResponse {
            job_id: job.id,
            status: job.status.as_str(),
        }),
    ))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let job = load_job(&state, id).await?;
    Ok(Json(job))
}

pub async fn wait_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(params): Query<WaitParams>,
) -> AppResult<impl IntoResponse> {
    let timeout_ms = params.timeout_ms.min(MAX_WAIT_MS);
    let notify = state.waiters.handle(id);

    let job = load_job(&state, id).await?;
    if job.status.is_terminal() {
        return Ok(Json(job));
    }

    let timeout = Duration::from_millis(timeout_ms);
    let notified = notify.notified();
    match tokio::time::timeout(timeout, notified).await {
        Ok(()) => {
            let job = load_job(&state, id).await?;
            if job.status.is_terminal() {
                Ok(Json(job))
            } else {
                Err(AppError::Internal(
                    "notified but job not terminal".into(),
                ))
            }
        }
        Err(_) => {
            let job = load_job(&state, id).await?;
            if job.status.is_terminal() {
                Ok(Json(job))
            } else {
                Err(AppError::WaitTimeout)
            }
        }
    }
}

async fn load_job(state: &AppState, id: Uuid) -> AppResult<Job> {
    if let Some(job) = state.cache.get(id).await?
        && job.status.is_terminal()
    {
        return Ok(job);
    }
    match state.ledger.get(id).await? {
        Some(job) => {
            if matches!(job.status, JobStatus::Completed | JobStatus::Failed) {
                let _ = state.cache.put(&job).await;
            }
            Ok(job)
        }
        None => Err(AppError::JobNotFound(id.to_string())),
    }
}

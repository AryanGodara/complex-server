use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use uuid::Uuid;

use crate::domain::job::Job;
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
    let timeout = Duration::from_millis(params.timeout_ms.min(MAX_WAIT_MS));
    let notify = state.waiters.handle(id);

    let job = load_job(&state, id).await?;
    if job.status.is_terminal() {
        return Ok(Json(job));
    }

    let timed_out = tokio::time::timeout(timeout, notify.notified()).await.is_err();
    let job = load_job(&state, id).await?;
    if job.status.is_terminal() {
        Ok(Json(job))
    } else if timed_out {
        Err(AppError::WaitTimeout)
    } else {
        Err(AppError::Internal("notified but job not terminal".into()))
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
            if job.status.is_terminal() {
                let _ = state.cache.put(&job).await;
            }
            Ok(job)
        }
        None => Err(AppError::JobNotFound(id.to_string())),
    }
}

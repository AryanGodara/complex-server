use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("job {0} not found")]
    JobNotFound(String),

    #[error("invalid job payload: {0}")]
    BadRequest(String),

    #[error("wait timed out")]
    WaitTimeout,

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("redis pool error: {0}")]
    RedisPool(String),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<deadpool_redis::PoolError> for AppError {
    fn from(value: deadpool_redis::PoolError) -> Self {
        AppError::RedisPool(value.to_string())
    }
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::JobNotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::WaitTimeout => StatusCode::REQUEST_TIMEOUT,
            AppError::Database(_)
            | AppError::Redis(_)
            | AppError::RedisPool(_)
            | AppError::Serde(_)
            | AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            AppError::JobNotFound(_) => "job_not_found",
            AppError::BadRequest(_) => "bad_request",
            AppError::WaitTimeout => "wait_timeout",
            AppError::Database(_) => "database_error",
            AppError::Redis(_) | AppError::RedisPool(_) => "redis_error",
            AppError::Serde(_) => "serialization_error",
            AppError::Internal(_) => "internal_error",
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = Json(json!({
            "error": self.code(),
            "message": self.to_string(),
        }));
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        } else {
            tracing::debug!(error = %self, "request rejected");
        }
        (status, body).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;

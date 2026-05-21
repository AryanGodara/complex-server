use std::time::Duration;

use axum::Router;
use axum::routing::{get, post};
use axum::http::StatusCode;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::http::handlers;
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(handlers::health))
        .route("/v1/jobs", post(handlers::submit))
        .route("/v1/jobs/{id}", get(handlers::get_job))
        .route("/v1/jobs/{id}/wait", get(handlers::wait_job))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(120),
        ))
}

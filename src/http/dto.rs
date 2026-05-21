use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::calculation::Calculation;

#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    #[serde(flatten)]
    pub calculation: Calculation,
}

#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub job_id: Uuid,
    pub status: &'static str,
}

#[derive(Debug, Deserialize, Default)]
pub struct WaitParams {
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    5_000
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub queue_depth: u64,
}

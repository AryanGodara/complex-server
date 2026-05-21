use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::domain::engine;
use crate::error::AppResult;
use crate::notify::waiters::WaiterRegistry;
use crate::queue::redis_queue::JobQueue;
use crate::storage::cache::ResultCache;
use crate::storage::ledger::JobLedger;

const BRPOP_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct WorkerDeps {
    pub ledger: JobLedger,
    pub queue: JobQueue,
    pub cache: ResultCache,
    pub waiters: WaiterRegistry,
}

pub fn spawn(deps: WorkerDeps, concurrency: usize, cancel: CancellationToken) -> Vec<JoinHandle<()>> {
    (0..concurrency)
        .map(|worker_id| {
            let deps = deps.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_worker(worker_id, deps, cancel).await;
            })
        })
        .collect()
}

async fn run_worker(worker_id: usize, deps: WorkerDeps, cancel: CancellationToken) {
    tracing::info!(worker_id, "worker started");
    loop {
        if cancel.is_cancelled() {
            break;
        }

        let popped = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            r = deps.queue.pop_blocking(BRPOP_TIMEOUT) => r,
        };

        match popped {
            Ok(Some(id)) => {
                if let Err(e) = process_one(&deps, id).await {
                    tracing::error!(worker_id, job_id = %id, error = %e, "job processing failed");
                }
            }
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(worker_id, error = %e, "queue pop failed; backing off");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    tracing::info!(worker_id, "worker stopped");
}

async fn process_one(deps: &WorkerDeps, id: Uuid) -> AppResult<()> {
    let Some(job) = deps.ledger.get(id).await? else {
        tracing::warn!(job_id = %id, "queued job not in ledger");
        return Ok(());
    };

    deps.ledger.mark_running(id).await?;

    let outcome = engine::execute(job.calculation.clone()).await;

    match outcome {
        Ok(result) => {
            deps.ledger.mark_completed(id, &result).await?;
        }
        Err(e) => {
            deps.ledger.mark_failed(id, &e.to_string()).await?;
        }
    }

    if let Ok(Some(updated)) = deps.ledger.get(id).await {
        let _ = deps.cache.put(&updated).await;
    }

    deps.waiters.notify(id);
    Ok(())
}

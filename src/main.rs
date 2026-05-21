use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use complex_server::config::AppConfig;
use complex_server::http::routes;
use complex_server::notify::waiters::WaiterRegistry;
use complex_server::queue::redis_queue::{JobQueue, build_pool};
use complex_server::shutdown;
use complex_server::state::AppState;
use complex_server::storage::cache::ResultCache;
use complex_server::storage::ledger::JobLedger;
use complex_server::worker::pool::{WorkerDeps, spawn as spawn_workers};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cfg = AppConfig::load().context("loading config")?;
    tracing::info!(bind = %cfg.server.bind, "starting complex-server");

    ensure_data_dir(&cfg.database.url)?;

    let ledger = JobLedger::connect(&cfg.database.url)
        .await
        .context("connecting to sqlite")?;

    let redis_pool =
        build_pool(&cfg.redis.url, cfg.redis.pool_size).context("creating redis pool")?;

    let queue = JobQueue::new(redis_pool.clone(), cfg.redis.queue_key.clone());
    let cache = ResultCache::new(
        redis_pool.clone(),
        cfg.redis.result_prefix.clone(),
        cfg.redis.result_ttl_seconds,
    );
    let waiters = WaiterRegistry::new();

    let state = AppState {
        ledger: ledger.clone(),
        queue: queue.clone(),
        cache: cache.clone(),
        waiters: waiters.clone(),
    };

    let cancel = CancellationToken::new();

    let worker_handles = spawn_workers(
        WorkerDeps {
            ledger,
            queue,
            cache,
            waiters,
        },
        cfg.worker.concurrency,
        cancel.clone(),
    );

    let listener = TcpListener::bind(&cfg.server.bind)
        .await
        .with_context(|| format!("binding to {}", cfg.server.bind))?;
    tracing::info!(addr = %listener.local_addr()?, "http listening");

    let shutdown_signal = {
        let cancel = cancel.clone();
        async move { shutdown::wait_for_signal(cancel).await }
    };

    axum::serve(listener, routes::router(state))
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context("axum server error")?;

    tracing::info!("http server stopped; draining workers");
    let grace = Duration::from_secs(cfg.worker.shutdown_grace_seconds);
    let drain = async {
        for h in worker_handles {
            let _ = h.await;
        }
    };
    if tokio::time::timeout(grace, drain).await.is_err() {
        tracing::warn!("workers did not drain within grace period");
    }
    tracing::info!("shutdown complete");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,complex_server=debug,tower_http=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

fn ensure_data_dir(database_url: &str) -> anyhow::Result<()> {
    let path = database_url
        .strip_prefix("sqlite://")
        .unwrap_or(database_url);
    let path = path.split('?').next().unwrap_or(path);
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating db parent dir {}", parent.display()))?;
    }
    Ok(())
}

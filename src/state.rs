use crate::notify::waiters::WaiterRegistry;
use crate::queue::redis_queue::JobQueue;
use crate::storage::cache::ResultCache;
use crate::storage::ledger::JobLedger;

#[derive(Clone)]
pub struct AppState {
    pub ledger: JobLedger,
    pub queue: JobQueue,
    pub cache: ResultCache,
    pub waiters: WaiterRegistry,
}

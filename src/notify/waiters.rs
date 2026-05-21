use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct WaiterRegistry {
    inner: Arc<DashMap<Uuid, Arc<Notify>>>,
}

impl WaiterRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn handle(&self, id: Uuid) -> Arc<Notify> {
        self.inner
            .entry(id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    pub fn notify(&self, id: Uuid) {
        if let Some((_, notify)) = self.inner.remove(&id) {
            notify.notify_waiters();
        }
    }
}

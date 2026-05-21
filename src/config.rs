use std::{env, fs, path::Path};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub worker: WorkerConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    pub pool_size: usize,
    pub queue_key: String,
    pub result_prefix: String,
    pub result_ttl_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub concurrency: usize,
    pub shutdown_grace_seconds: u64,
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        let path =
            env::var("COMPLEX_SERVER_CONFIG").unwrap_or_else(|_| "config/default.toml".to_string());
        let raw = fs::read_to_string(Path::new(&path))?;
        let mut cfg: AppConfig = toml::from_str(&raw)?;
        cfg.apply_env_overrides();
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = env::var("COMPLEX_SERVER__SERVER__BIND") {
            self.server.bind = v;
        }
        if let Ok(v) = env::var("COMPLEX_SERVER__DATABASE__URL") {
            self.database.url = v;
        }
        if let Ok(v) = env::var("COMPLEX_SERVER__REDIS__URL") {
            self.redis.url = v;
        }
        if let Ok(v) = env::var("COMPLEX_SERVER__REDIS__POOL_SIZE")
            && let Ok(n) = v.parse::<usize>()
        {
            self.redis.pool_size = n;
        }
        if let Ok(v) = env::var("COMPLEX_SERVER__WORKER__CONCURRENCY")
            && let Ok(n) = v.parse::<usize>()
        {
            self.worker.concurrency = n;
        }
    }
}

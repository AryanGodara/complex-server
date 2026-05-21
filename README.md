# complex-server

Async calculation server in Rust. Submit a calculation, get a job id, poll or long-poll for the result. SQLite is the ledger of record; Redis is the queue and the hot read cache.

## Stack

- axum 0.8 on tokio
- sqlx + SQLite (WAL, embedded migrations)
- Redis (queue via LPUSH/BRPOP, results via SETEX)
- deadpool-redis connection pool
- tower-http (trace, timeout)
- tracing-subscriber

## Run

```sh
docker compose up -d redis
cargo run --release
```

That's it. The default config binds `0.0.0.0:8080`, opens `data/ledger.db`, and connects to Redis on `127.0.0.1:6379`.

Override anything via env vars (double underscore between sections):

```sh
COMPLEX_SERVER__SERVER__BIND=127.0.0.1:9000 \
COMPLEX_SERVER__WORKER__CONCURRENCY=8 \
cargo run --release
```

## API

### Submit a job

```sh
curl -s localhost:8080/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"kind":"fibonacci","payload":{"n":100}}'
```

Returns:

```json
{"job_id":"...","status":"queued"}
```

Supported kinds:

- `fibonacci` — `{ "n": u64 }` (n <= 200000)
- `prime_factors` — `{ "n": u64 }` (n >= 2, n <= 2^62)
- `matrix_multiply` — `{ "a": [[f64]], "b": [[f64]] }` (dims <= 256)
- `sleep` — `{ "ms": u64 }` (ms <= 60000)

### Poll a job

```sh
curl -s localhost:8080/v1/jobs/<id>
```

### Long-poll until terminal (or timeout)

```sh
curl -s "localhost:8080/v1/jobs/<id>/wait?timeout_ms=5000"
```

Returns the job once it reaches `completed` or `failed`, or `408` if the wait exceeded `timeout_ms` (capped server-side at 60s).

### Health

```sh
curl -s localhost:8080/healthz
```

## Layout

```
src/
  main.rs            wiring + graceful shutdown
  config.rs          TOML + env overrides
  error.rs           AppError -> IntoResponse
  state.rs           shared handle for axum State
  shutdown.rs        SIGINT/SIGTERM trap
  domain/
    job.rs           Job, JobStatus
    calculation.rs   Calculation, CalculationResult
    engine.rs        execute() with spawn_blocking
  storage/
    ledger.rs        sqlx + sqlite (jobs table)
    cache.rs         redis SETEX for terminal jobs
  queue/
    redis_queue.rs   LPUSH/BRPOP queue
  worker/
    pool.rs          N tokio tasks, BRPOP loop
  notify/
    waiters.rs       DashMap<Uuid, Notify>
  http/
    routes.rs        axum Router
    handlers.rs      submit, get, wait, health
    dto.rs           request/response shapes

migrations/
  0001_init.sql      jobs table + indexes
config/
  default.toml       baked-in defaults
```

See `DESIGN.md` for the full architecture write-up.

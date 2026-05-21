CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY NOT NULL,
    kind          TEXT NOT NULL,
    payload       TEXT NOT NULL,
    status        TEXT NOT NULL,
    result        TEXT,
    error         TEXT,
    created_at    INTEGER NOT NULL,
    started_at    INTEGER,
    completed_at  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_status      ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at  ON jobs(created_at);

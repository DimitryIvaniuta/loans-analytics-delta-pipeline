CREATE TABLE IF NOT EXISTS job_run (
  id              UUID PRIMARY KEY,
  as_of_date       DATE NOT NULL,
  status           VARCHAR(32) NOT NULL,
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  error_message    TEXT
);

CREATE TABLE IF NOT EXISTS job_run_feed (
  id              BIGSERIAL PRIMARY KEY,
  job_run_id      UUID NOT NULL REFERENCES job_run(id) ON DELETE CASCADE,
  feed_name       VARCHAR(64) NOT NULL,
  status          VARCHAR(32) NOT NULL,
  source_file     TEXT,
  staged_rows     BIGINT NOT NULL DEFAULT 0,
  snapshot_rows   BIGINT NOT NULL DEFAULT 0,
  delta_rows      BIGINT NOT NULL DEFAULT 0,
  error_message   TEXT,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_job_run_asof ON job_run(as_of_date);
CREATE INDEX IF NOT EXISTS ix_job_run_feed_run ON job_run_feed(job_run_id);
CREATE INDEX IF NOT EXISTS ix_job_run_feed_name ON job_run_feed(feed_name);


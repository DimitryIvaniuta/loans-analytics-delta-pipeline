CREATE TABLE IF NOT EXISTS ingest_reject (
  id            BIGSERIAL PRIMARY KEY,
  job_run_id    UUID NOT NULL REFERENCES job_run(id) ON DELETE CASCADE,
  feed_name     VARCHAR(64) NOT NULL,
  as_of_date    DATE NOT NULL,
  source_file   TEXT,
  reason        TEXT NOT NULL,
  raw_row       TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ingest_reject_run ON ingest_reject(job_run_id);
CREATE INDEX IF NOT EXISTS ix_ingest_reject_asof_feed ON ingest_reject(as_of_date, feed_name);


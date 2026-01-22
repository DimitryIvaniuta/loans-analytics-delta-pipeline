CREATE TABLE IF NOT EXISTS delta_event (
  id              BIGSERIAL PRIMARY KEY,
  job_run_id      UUID NOT NULL REFERENCES job_run(id) ON DELETE CASCADE,
  feed_name       VARCHAR(64) NOT NULL,
  as_of_date      DATE NOT NULL,
  op              CHAR(1) NOT NULL CHECK (op IN ('I','U','D')),
  entity_key      JSONB NOT NULL,
  before_row      JSONB,
  after_row       JSONB,
  changed_fields  JSONB,
  old_row_hash    TEXT,
  new_row_hash    TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_delta_event_asof_feed ON delta_event(as_of_date, feed_name);
CREATE INDEX IF NOT EXISTS ix_delta_event_run ON delta_event(job_run_id);


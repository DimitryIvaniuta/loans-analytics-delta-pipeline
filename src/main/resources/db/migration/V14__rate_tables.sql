CREATE TABLE IF NOT EXISTS stg_rate (
  job_run_id       UUID NOT NULL,
  as_of_date       DATE NOT NULL,
  source_file      TEXT,
  loan_id          TEXT,
  rate_type        TEXT,
  index_name       TEXT,
  margin           NUMERIC,
  current_rate     NUMERIC,
  next_reset_date  DATE,
  cap              NUMERIC,
  floor            NUMERIC
);

CREATE TABLE IF NOT EXISTS snap_rate (
  as_of_date       DATE NOT NULL,
  loan_id          TEXT NOT NULL,
  rate_type        TEXT,
  index_name       TEXT,
  margin           NUMERIC,
  current_rate     NUMERIC,
  next_reset_date  DATE,
  cap              NUMERIC,
  floor            NUMERIC,

  row_hash          TEXT NOT NULL,
  source_file       TEXT,
  ingested_at       TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, loan_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_rate_loan ON snap_rate(loan_id);

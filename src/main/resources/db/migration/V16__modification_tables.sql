CREATE TABLE IF NOT EXISTS stg_modification (
  job_run_id        UUID NOT NULL,
  as_of_date        DATE NOT NULL,
  source_file       TEXT,
  modification_id   TEXT,
  loan_id           TEXT,
  modification_type TEXT,
  effective_date    DATE,
  new_interest_rate NUMERIC,
  new_term_months   INT,
  reason            TEXT,
  status            TEXT
);

CREATE TABLE IF NOT EXISTS snap_modification (
  as_of_date        DATE NOT NULL,
  modification_id   TEXT NOT NULL,
  loan_id           TEXT,
  modification_type TEXT,
  effective_date    DATE,
  new_interest_rate NUMERIC,
  new_term_months   INT,
  reason            TEXT,
  status            TEXT,

  row_hash           TEXT NOT NULL,
  source_file        TEXT,
  ingested_at        TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, modification_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_modification_loan ON snap_modification(loan_id);

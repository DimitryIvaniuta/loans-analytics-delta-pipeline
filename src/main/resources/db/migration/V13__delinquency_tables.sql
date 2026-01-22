CREATE TABLE IF NOT EXISTS stg_delinquency (
  job_run_id         UUID NOT NULL,
  as_of_date         DATE NOT NULL,
  source_file        TEXT,
  loan_id            TEXT,
  days_past_due      INT,
  delinquency_bucket TEXT,
  next_action        TEXT,
  next_action_date   DATE,
  hardship_flag      BOOLEAN
);

CREATE TABLE IF NOT EXISTS snap_delinquency (
  as_of_date         DATE NOT NULL,
  loan_id            TEXT NOT NULL,
  days_past_due      INT,
  delinquency_bucket TEXT,
  next_action        TEXT,
  next_action_date   DATE,
  hardship_flag      BOOLEAN,

  row_hash           TEXT NOT NULL,
  source_file        TEXT,
  ingested_at        TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, loan_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_delinquency_loan ON snap_delinquency(loan_id);

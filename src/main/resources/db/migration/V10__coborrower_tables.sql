CREATE TABLE IF NOT EXISTS stg_coborrower (
  job_run_id     UUID NOT NULL,
  as_of_date     DATE NOT NULL,
  source_file    TEXT,
  loan_id        TEXT,
  coborrower_id  TEXT,
  first_name     TEXT,
  last_name      TEXT,
  date_of_birth  DATE,
  relationship   TEXT,
  email          TEXT,
  phone          TEXT
);

CREATE TABLE IF NOT EXISTS snap_coborrower (
  as_of_date     DATE NOT NULL,
  loan_id        TEXT NOT NULL,
  coborrower_id  TEXT NOT NULL,
  first_name     TEXT,
  last_name      TEXT,
  date_of_birth  DATE,
  relationship   TEXT,
  email          TEXT,
  phone          TEXT,

  row_hash       TEXT NOT NULL,
  source_file    TEXT,
  ingested_at    TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, loan_id, coborrower_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_coborrower_loan ON snap_coborrower(loan_id);

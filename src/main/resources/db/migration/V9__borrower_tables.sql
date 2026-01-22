CREATE TABLE IF NOT EXISTS stg_borrower (
  job_run_id       UUID NOT NULL,
  as_of_date       DATE NOT NULL,
  source_file      TEXT,
  borrower_id      TEXT,
  first_name       TEXT,
  last_name        TEXT,
  date_of_birth    DATE,
  national_id_hash TEXT,
  email            TEXT,
  phone            TEXT,
  employer         TEXT,
  annual_income    NUMERIC,
  created_date     TIMESTAMPTZ,
  modified_date    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS snap_borrower (
  as_of_date       DATE NOT NULL,
  borrower_id      TEXT NOT NULL,
  first_name       TEXT,
  last_name        TEXT,
  date_of_birth    DATE,
  national_id_hash TEXT,
  email            TEXT,
  phone            TEXT,
  employer         TEXT,
  annual_income    NUMERIC,
  created_date     TIMESTAMPTZ,
  modified_date    TIMESTAMPTZ,

  row_hash         TEXT NOT NULL,
  source_file      TEXT,
  ingested_at      TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, borrower_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_borrower_id ON snap_borrower(borrower_id);

CREATE TABLE IF NOT EXISTS stg_loan_master (
  job_run_id         UUID NOT NULL,
  as_of_date         DATE NOT NULL,
  source_file        TEXT,
  loan_id            TEXT,
  borrower_id        TEXT,
  product_code       TEXT,
  status             TEXT,
  origination_date   DATE,
  maturity_date      DATE,
  principal_balance  NUMERIC,
  currency           TEXT,
  interest_rate      NUMERIC,
  ltv                NUMERIC,
  branch_id          TEXT,
  region             TEXT,
  last_modified_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS snap_loan_master (
  as_of_date         DATE NOT NULL,
  loan_id            TEXT NOT NULL,
  borrower_id        TEXT,
  product_code       TEXT,
  status             TEXT,
  origination_date   DATE,
  maturity_date      DATE,
  principal_balance  NUMERIC,
  currency           TEXT,
  interest_rate      NUMERIC,
  ltv                NUMERIC,
  branch_id          TEXT,
  region             TEXT,
  last_modified_at   TIMESTAMPTZ,

  row_hash           TEXT  NOT NULL,
  source_file        TEXT,
  ingested_at        TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, loan_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_loan_master_loan ON snap_loan_master(loan_id);

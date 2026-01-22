CREATE TABLE IF NOT EXISTS stg_payment_transaction (
  job_run_id         UUID NOT NULL,
  as_of_date         DATE NOT NULL,
  source_file        TEXT,
  transaction_id     TEXT,
  loan_id            TEXT,
  transaction_date   DATE,
  posting_date       DATE,
  transaction_type   TEXT,
  amount             NUMERIC,
  currency           TEXT,
  channel            TEXT,
  reference          TEXT
);

CREATE TABLE IF NOT EXISTS snap_payment_transaction (
  as_of_date         DATE NOT NULL,
  transaction_id     TEXT NOT NULL,
  loan_id            TEXT,
  transaction_date   DATE,
  posting_date       DATE,
  transaction_type   TEXT,
  amount             NUMERIC,
  currency           TEXT,
  channel            TEXT,
  reference          TEXT,

  row_hash           TEXT  NOT NULL,
  source_file        TEXT,
  ingested_at        TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, transaction_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_payment_tx_loan ON snap_payment_transaction(loan_id);

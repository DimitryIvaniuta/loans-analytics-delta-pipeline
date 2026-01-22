CREATE TABLE IF NOT EXISTS stg_payment_schedule (
  job_run_id      UUID NOT NULL,
  as_of_date      DATE NOT NULL,
  source_file     TEXT,
  loan_id         TEXT,
  installment_no  INT,
  due_date        DATE,
  due_amount      NUMERIC,
  principal_due   NUMERIC,
  interest_due    NUMERIC,
  escrow_due      NUMERIC,
  status          TEXT
);

CREATE TABLE IF NOT EXISTS snap_payment_schedule (
  as_of_date      DATE NOT NULL,
  loan_id         TEXT NOT NULL,
  installment_no  INT  NOT NULL,
  due_date        DATE,
  due_amount      NUMERIC,
  principal_due   NUMERIC,
  interest_due    NUMERIC,
  escrow_due      NUMERIC,
  status          TEXT,

  row_hash         TEXT NOT NULL,
  source_file      TEXT,
  ingested_at      TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, loan_id, installment_no)
);

CREATE INDEX IF NOT EXISTS ix_snap_payment_schedule_loan ON snap_payment_schedule(loan_id);

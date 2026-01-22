CREATE TABLE IF NOT EXISTS stg_escrow (
  job_run_id        UUID NOT NULL,
  as_of_date        DATE NOT NULL,
  source_file       TEXT,
  loan_id           TEXT,
  escrow_balance    NUMERIC,
  tax_reserve       NUMERIC,
  insurance_reserve NUMERIC,
  hazard_policy_no  TEXT,
  hazard_premium    NUMERIC,
  flood_policy_no   TEXT,
  flood_premium     NUMERIC
);

CREATE TABLE IF NOT EXISTS snap_escrow (
  as_of_date        DATE NOT NULL,
  loan_id           TEXT NOT NULL,
  escrow_balance    NUMERIC,
  tax_reserve       NUMERIC,
  insurance_reserve NUMERIC,
  hazard_policy_no  TEXT,
  hazard_premium    NUMERIC,
  flood_policy_no   TEXT,
  flood_premium     NUMERIC,

  row_hash           TEXT NOT NULL,
  source_file        TEXT,
  ingested_at        TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, loan_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_escrow_loan ON snap_escrow(loan_id);

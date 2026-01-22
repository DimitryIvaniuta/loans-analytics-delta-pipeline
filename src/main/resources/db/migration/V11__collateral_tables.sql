CREATE TABLE IF NOT EXISTS stg_collateral (
  job_run_id       UUID NOT NULL,
  as_of_date       DATE NOT NULL,
  source_file      TEXT,
  collateral_id    TEXT,
  loan_id          TEXT,
  property_type    TEXT,
  street           TEXT,
  city             TEXT,
  state            TEXT,
  postal_code      TEXT,
  country          TEXT,
  valuation_amount NUMERIC,
  valuation_date   DATE,
  occupancy        TEXT,
  year_built       INT
);

CREATE TABLE IF NOT EXISTS snap_collateral (
  as_of_date       DATE NOT NULL,
  collateral_id    TEXT NOT NULL,
  loan_id          TEXT,
  property_type    TEXT,
  street           TEXT,
  city             TEXT,
  state            TEXT,
  postal_code      TEXT,
  country          TEXT,
  valuation_amount NUMERIC,
  valuation_date   DATE,
  occupancy        TEXT,
  year_built       INT,

  row_hash         TEXT NOT NULL,
  source_file      TEXT,
  ingested_at      TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, collateral_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_collateral_loan ON snap_collateral(loan_id);

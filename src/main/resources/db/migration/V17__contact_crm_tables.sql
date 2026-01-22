CREATE TABLE IF NOT EXISTS stg_contact_crm (
  job_run_id        UUID NOT NULL,
  as_of_date        DATE NOT NULL,
  source_file       TEXT,

  contact_id        TEXT,
  email             TEXT,
  secondary_email   TEXT,
  office_phone      TEXT,
  home_phone        TEXT,
  cell_phone        TEXT,

  first_name        TEXT,
  last_name         TEXT,
  company           TEXT,
  position          TEXT,

  city              TEXT,
  state_province    TEXT,
  zip_postal_code   TEXT,
  country           TEXT,

  created_date      TIMESTAMPTZ,
  modified_date     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS snap_contact_crm (
  as_of_date        DATE NOT NULL,
  contact_id        TEXT NOT NULL,

  email             TEXT,
  secondary_email   TEXT,
  office_phone      TEXT,
  home_phone        TEXT,
  cell_phone        TEXT,

  first_name        TEXT,
  last_name         TEXT,
  company           TEXT,
  position          TEXT,

  city              TEXT,
  state_province    TEXT,
  zip_postal_code   TEXT,
  country           TEXT,

  created_date      TIMESTAMPTZ,
  modified_date     TIMESTAMPTZ,

  row_hash          TEXT NOT NULL,
  source_file       TEXT,
  ingested_at       TIMESTAMPTZ,

  PRIMARY KEY (as_of_date, contact_id)
);

CREATE INDEX IF NOT EXISTS ix_snap_contact_crm_id ON snap_contact_crm(contact_id);

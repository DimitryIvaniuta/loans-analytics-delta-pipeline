ALTER TABLE job_run
ALTER COLUMN started_at TYPE timestamptz
  USING started_at AT TIME ZONE 'UTC';
-- pgcrypto already enabled -> gen_random_uuid() is available
ALTER TABLE job_run_feed
    ALTER COLUMN id SET DEFAULT gen_random_uuid();
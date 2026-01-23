-- 1) Drop old PK on id (constraint name may differ)
ALTER TABLE job_run_feed DROP CONSTRAINT IF EXISTS job_run_feed_pkey;

-- 2) Remove surrogate id (optional but recommended)
ALTER TABLE job_run_feed DROP COLUMN IF EXISTS id;

-- 3) Enforce natural uniqueness
ALTER TABLE job_run_feed
    ADD CONSTRAINT job_run_feed_pkey PRIMARY KEY (job_run_id, feed_name);

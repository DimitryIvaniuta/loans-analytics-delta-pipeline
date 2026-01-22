CREATE OR REPLACE FUNCTION jsonb_diff(before JSONB, after JSONB)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
AS $$
  WITH keys AS (
    SELECT jsonb_object_keys(COALESCE(before, '{}'::jsonb)) AS k
    UNION
    SELECT jsonb_object_keys(COALESCE(after, '{}'::jsonb)) AS k
  )
  SELECT COALESCE(
    jsonb_object_agg(
      k,
      jsonb_build_object('before', before -> k, 'after', after -> k)
    ) FILTER (WHERE (before -> k) IS DISTINCT FROM (after -> k)),
    '{}'::jsonb
  )
  FROM keys;
$$;


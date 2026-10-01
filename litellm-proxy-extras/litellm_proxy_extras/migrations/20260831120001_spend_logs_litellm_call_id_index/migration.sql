-- The litellm_call_id index on LiteLLM_SpendLogs is built at proxy startup, after
-- migrate deploy, by litellm_proxy_extras/request_log_indexes.py: concurrently on a
-- plain table and per partition on a partitioned one. Postgres refuses CREATE INDEX
-- CONCURRENTLY on a partitioned parent, so this migration no longer runs it.
SELECT 1;

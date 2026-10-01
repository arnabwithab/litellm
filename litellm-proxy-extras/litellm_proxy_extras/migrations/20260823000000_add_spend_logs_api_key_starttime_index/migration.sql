-- The (api_key, startTime) index on LiteLLM_SpendLogs is built at proxy startup, after
-- migrate deploy, by litellm_proxy_extras/request_log_indexes.py: concurrently on a
-- plain table and per partition on a partitioned one. A migration cannot do either
-- without blocking spend-log writes or failing on a partitioned table.
SELECT 1;

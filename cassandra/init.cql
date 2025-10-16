CREATE KEYSPACE IF NOT EXISTS github_events
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS github_events.avg_pr_time (
    repo_name TEXT PRIMARY KEY,
    avg_interval_seconds DOUBLE,
    pr_count DOUBLE,
    last_pull_request_ts TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_counts_by_type (
    type text,
    bucket_day date, -- group buckets per day to keep partitions small
    time_bucket timestamp,
    count int,
    PRIMARY KEY ((type, bucket_day), time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC);
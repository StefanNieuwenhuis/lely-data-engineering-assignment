CREATE KEYSPACE IF NOT EXISTS github_events
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS github_events.avg_pr_time (
    repo_name TEXT PRIMARY KEY,
    avg_interval_seconds DOUBLE,
    pr_count DOUBLE,
    last_pull_request_ts TIMESTAMP,
    updated_at TIMESTAMP
);

INSERT INTO github_events.avg_pr_time (repo_name, avg_interval_seconds, pr_count, last_pull_request_ts, updated_at)
VALUES ('microsoft/vscode', 28950.4, 132, '2025-10-16T22:15:00', toTimestamp(now()));

CREATE TABLE IF NOT EXISTS event_counts_by_type (
    type text,
    bucket_day date -- group buckets per day to keep partitions small,
    time_bucket timestamp,
    count int,
    PRIMARY KEY ((type, bucket_day), time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC);


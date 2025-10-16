CREATE KEYSPACE IF NOT EXISTS github_events
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS github_events.avg_pr_time (
    repo_name TEXT PRIMARY KEY,
    avg_interval_seconds DOUBLE,
    pr_count DOUBLE,
    last_pull_request_ts TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE event_counts_by_type (
    event_type text,
    created_at timestamp,
    PRIMARY KEY (event_type, created_at)
);
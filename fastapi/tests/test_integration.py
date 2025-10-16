from datetime import datetime, timezone, timedelta

import pytest

from main import app
from src.database import CassandraConnection
from src.config import CASSANDRA_KEYSPACE

TEST_CASSANDRA_KEYSPACE = f"{CASSANDRA_KEYSPACE}_test"

SECONDS_PER_DAY = float(24 * 60 * 60)

def test_get_avg_pr_interval(client, test_session) -> None:
    """Test FastAPI endpoint should fetch Average Pull Request Intervals correctly"""

    MOCK_REPO_NAME = "test-org/test-repo"
    MOCK_PR_COUNT = 3

    now = datetime.now(timezone.utc)
    test_session.execute("""
        INSERT INTO avg_pr_time (repo_name, pr_count, avg_interval_seconds, last_pull_request_ts, updated_at)
            VALUES (%s, %s, %s, %s, %s)
    """, (MOCK_REPO_NAME, MOCK_PR_COUNT, SECONDS_PER_DAY, now - timedelta(seconds=SECONDS_PER_DAY), now))

    response = client.get(f"/v1/avg_pr_interval/{MOCK_REPO_NAME}")
    assert response.status_code == 200

    data = response.json()
    assert data["repo_name"] == MOCK_REPO_NAME
    assert data["avg_interval_seconds"] == SECONDS_PER_DAY
    assert data["pr_count"] == MOCK_PR_COUNT

def test_get_agg_event_counts(client, test_session) -> None:
    """Test FastAPI endpoint should fetch Aggregated Event Counts for a given offset correctly"""

    MOCK_OFFSET_MINUTES = 10


    test_session.execute("""
            INSERT INTO event_counts_by_type (type, bucket_day, time_bucket, count)
                VALUES (%s, %s, %s, %s, %s)
        """, ("PushEvent", "2025-10-16", "2025-10-16 16:55:00.000000+0000", 27))

    response = client.get(f"/v1/agg_event_counts?offset_minutes={MOCK_OFFSET_MINUTES}")
    assert response.status_code == 200


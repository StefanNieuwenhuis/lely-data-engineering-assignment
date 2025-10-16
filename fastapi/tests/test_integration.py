from datetime import datetime, timezone, timedelta

import pytest


from main import app
from src.database import CassandraConnection
from src.config import CASSANDRA_KEYSPACE

TEST_CASSANDRA_KEYSPACE = f"{CASSANDRA_KEYSPACE}_test"

SECONDS_PER_DAY = float(24 * 60 * 60)
MOCK_REPO_NAME = "test-org/test-repo"
MOCK_PR_COUNT = 3

def test_get_avg_pr_interval(client, test_session) -> None:
    """Test FastAPI endpoint should fetch Average Pull Request Intervals correctly"""

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

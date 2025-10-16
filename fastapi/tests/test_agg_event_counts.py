import pytest
from datetime import datetime, timezone, timedelta

from cassandra.query import SimpleStatement

from src.config import GITHUB_EVENTS_TYPES

MOCK_OFFSET_MINUTES=10

def insert_event_count(session, event_type: str, count: int, time_bucket: datetime) -> None:
    """Helper function to insert mock data into the database"""

    stmt = SimpleStatement("""
        INSERT INTO event_counts_by_type (type, bucket_day, time_bucket, count)
            VALUES (%s, %s, %s, %s)
    """)

    session.execute(stmt, (event_type, time_bucket.date(), time_bucket, count))

def test_counts_within_offset(client, test_session) -> None:
    """It should aggregate and count the events in the offset correctly"""
    now = datetime.now(timezone.utc)

    insert_event_count(test_session, "PushEvent", 5, now - timedelta(minutes=5))
    insert_event_count(test_session, "PullRequestEvent", 3, now - timedelta(minutes=2))
    insert_event_count(test_session, "PushEvent", 2, now - timedelta(minutes=1))

    response = client.get(f"/v1/agg_event_counts?offset_minutes={MOCK_OFFSET_MINUTES}")
    assert response.status_code == 200

    data = response.json()

    assert data["counts"]["PushEvent"] == 7
    assert data["counts"]["PullRequestEvent"] == 3

def test_count_no_events(client, test_session) -> None:
    """It should return no counts when no events are registered in the offset"""
    now = datetime.now(timezone.utc)

    # Insert events that are older than the offset
    insert_event_count(test_session, "PushEvent", 5, now - timedelta(minutes=20))
    insert_event_count(test_session, "PullRequestEvent", 3, now - timedelta(minutes=15))

    response = client.get(f"/v1/agg_event_counts?offset_minutes={MOCK_OFFSET_MINUTES}")
    assert response.status_code == 200

    data = response.json()

    for github_event in GITHUB_EVENTS_TYPES.split(","):
        assert data["counts"][github_event] == 0

def test_counts_cross_day_boundary(client, test_session):
    """It should count events crossing day boundaries correctly"""
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1, minutes=5)

    insert_event_count(test_session, "PushEvent", 2, yesterday)
    insert_event_count(test_session, "PushEvent", 3, now - timedelta(minutes=2))

    response = client.get(f"/v1/agg_event_counts?offset_minutes={MOCK_OFFSET_MINUTES}")
    assert response.status_code == 200

    data = response.json()

    # Only event within last 10 minutes counted
    assert data["counts"]["PushEvent"] == 3

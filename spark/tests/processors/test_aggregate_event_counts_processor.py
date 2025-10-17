import pytest

from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession, Row

from src.processors import AggregateEventCountsProcessor


@pytest.fixture
def processor() -> "AggregateEventCountsProcessor":
    """Provide a new AveragePRIntervalProcessor instance before each test"""
    return AggregateEventCountsProcessor()

@pytest.fixture(scope="module")
def sample_events(spark_session: SparkSession) -> DataFrame:
    base_time = datetime(2025,10,16,12,0,0)

    data = [
        Row(type="PushEvent", created_at=(base_time + timedelta(seconds=10)).isoformat()),
        Row(type="PushEvent", created_at=(base_time + timedelta(seconds=30)).isoformat()),
        Row(type="PullRequestEvent", created_at=(base_time + timedelta(seconds=90)).isoformat()),
        Row(type="PushEvent", created_at=(base_time + timedelta(seconds=80)).isoformat()),
    ]

    return spark_session.createDataFrame(data)


def test_aggregate_event_counts_basic(spark_session, processor, sample_events) -> None:
    """It should aggregate event counts for each event type successfully"""

    result_df = processor.run(sample_events, window_duration="1 minute")
    results_dict = [row.asDict() for row in result_df.collect()]


    # Assert schema correctness
    expected_columns = {"type", "bucket_day", "time_bucket", "count"}
    assert expected_columns.issubset(result_df.columns)

    # Assert counts per event type
    push_event_counts = [r["count"] for r in results_dict if r["type"] == "PushEvent"]
    pull_request_event_counts = [r["count"] for r in results_dict if r["type"] == "PullRequestEvent"]

    # there should be two windows for push events with 3 push events in total
    assert len(push_event_counts) == 2
    assert sum(push_event_counts) == 3

    assert len(pull_request_event_counts) == 1

    for r in results_dict:
        assert isinstance(r["time_bucket"], datetime)
        assert r["bucket_day"] == datetime(2025, 10, 16).date()



def test_aggregate_event_counts_empty_df(spark_session, processor, sample_events) -> None:
    """It should return an empty DataFrame when no events are present"""
    empty_df = spark_session.createDataFrame([], schema="type STRING, created_at STRING")
    result_df = processor.run(empty_df)

    assert result_df.count() == 0
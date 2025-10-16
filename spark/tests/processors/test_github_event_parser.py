import pytest

from src.processors import GitHubEventParser

from src.schemas import github_event_schema, github_event_field_map


@pytest.fixture
def kafka_input_df(spark_session):
    """Mock Kafka source DataFrame with json in 'value' column"""
    data = [
        ('{"type": "PushEvent", "created_at": "2025-10-16T10:00:00Z", "repo": { "name": "apache/spark" }}',),
        ('{"type": "PullRequestEvent", "created_at": "2025-10-16T11:00:00Z", "repo": { "name": "apache/kafka" }}',),
    ]

    return spark_session.createDataFrame(data, ["value"])

def test_parse_valid_json(spark_session, kafka_input_df) -> None:
    """It should parse Kafka JSON input correctly"""

    parsed_df = GitHubEventParser.parse(kafka_input_df, github_event_schema, github_event_field_map)
    result = parsed_df.collect()

    assert len(result) == 2
    assert result[0]["type"] == "PushEvent"
    assert result[1]["type"] == "PullRequestEvent"

def test_parse_handle_missing_fields(spark_session) -> None:
    """It should handle JSON input with missing fields correctly - i.e. None"""
    data = [{"type": "PushEvent"}]
    df = spark_session.createDataFrame(data, ["value"])

    parsed_df = GitHubEventParser.parse(df, github_event_schema, github_event_field_map)
    result = parsed_df.collect()[0]

    assert result["repo_name"] is None
    assert result["created_at"] is None

def test_parse_malformed_json(spark_session) -> None:
    """It should handle malformed JSON input correctly - i.e. produces None, no errors"""
    data = [('{"type": "PushEvent",',)]  # malformed JSON
    df = spark_session.createDataFrame(data, ["value"])

    parsed_df = GitHubEventParser.parse(df, github_event_schema, github_event_field_map)
    result = parsed_df.collect()[0]

    # Expect all fields to be None because JSON couldn't be parsed
    assert result["type"] is None
    assert result["repo_name"] is None
    assert result["created_at"] is None

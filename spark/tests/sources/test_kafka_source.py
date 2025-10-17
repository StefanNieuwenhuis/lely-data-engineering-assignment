from ensurepip import bootstrap
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame

from src.sources import KafkaStreamReader
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NAME



@pytest.fixture
def mock_spark_session() -> MagicMock:
    """Create a mock Spark Session containing a (mocked) readStream chain"""
    mock_spark_session = MagicMock()
    mock_df = MagicMock()
    mock_reader = MagicMock()

    # Chain configuration

    mock_reader.option.return_value = mock_reader  # option returns itself for chaining
    mock_reader.format.return_value = mock_reader  # format returns itself for chaining
    mock_reader.load.return_value = mock_df  # load returns the final DataFrame
    mock_spark_session.readStream = mock_reader

    return mock_spark_session

def test_read_stream_builds_correct_query(mock_spark_session) -> None:
    """It should call PySpark ReadStream with correct Kafka options"""
    mock_starting_offsets = "earliest"
    reader = KafkaStreamReader(mock_spark_session, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    result = reader.read(topic=KAFKA_TOPIC_NAME, starting_offsets=mock_starting_offsets)

    mock_spark_session.readStream.format.assert_called_once_with("kafka")

    expected_calls = [
        ("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS),
        ("subscribe", KAFKA_TOPIC_NAME),
        ("startingOffsets", mock_starting_offsets)
    ]
    actual_calls = mock_spark_session.readStream.format.return_value.option.call_args_list

    # flatten and compare
    assert [c[0] for c in actual_calls] == expected_calls
    mock_spark_session.readStream.format.return_value.load.assert_called_once()

    assert result is reader

def test_to_df_before_read_returns_none(mock_spark_session):
    """It should return None if .read() hasn't been called yet."""

    reader = KafkaStreamReader(mock_spark_session, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    assert reader.to_df() is None
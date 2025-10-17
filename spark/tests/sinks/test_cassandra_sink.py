import pytest
import time
import logging

from unittest.mock import MagicMock, patch, ANY
from pyspark.sql import DataFrame

from src.sinks import CassandraSink, upsert
from src.config import CASSANDRA_KEYSPACE

MOCK_BATCH_ID = 42
MOCK_CASSANDRA_KEYSPACE = f"{CASSANDRA_KEYSPACE}_test"
MOCK_TABLE_NAME = "test_table"
MOCK_CHECKPOINT_ROOT = "/tmp/checkpoints"

def test_upsert_basic(caplog) -> None:
    """It should call DataFrame.write.format().save() for non-empty batches"""
    caplog.set_level(logging.INFO)

    mock_df = MagicMock(spec=DataFrame)
    mock_df.isEmpty.return_value = False
    mock_df.count.return_value = 3

    # Mock the write chain
    mock_writer = MagicMock()
    mock_df.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    upsert(batch_df=mock_df, batch_id = MOCK_BATCH_ID, table=MOCK_TABLE_NAME)

    assert "Writing 3 rows" in caplog.text
    mock_df.write.format.assert_called_once_with("org.apache.spark.sql.cassandra")
    mock_writer.save.assert_called_once()


def test_upsert_skips_empty_batches(caplog) -> None:
    """It should skip writing when DataFrame is empty"""
    caplog.set_level(logging.INFO)

    mock_df = MagicMock(spec=DataFrame)
    mock_df.isEmpty.return_value = True

    upsert(batch_df=mock_df, batch_id = MOCK_BATCH_ID, table=MOCK_TABLE_NAME)

    assert "skipping write" in caplog.text
    mock_df.write.format.assert_not_called()

def test_construct_checkpoint_path(monkeypatch) -> None:
    """It should generate a checkpoint path containing keyspace, table and timestamp"""
    sink = CassandraSink(MOCK_CASSANDRA_KEYSPACE, checkpoint_root=MOCK_CHECKPOINT_ROOT)

    mock_time_value = 1234567890
    monkeypatch.setattr(time, "time", lambda: mock_time_value)

    path = sink._construct_checkpoint_path(MOCK_TABLE_NAME)

    assert path == f"{MOCK_CHECKPOINT_ROOT}/{MOCK_CASSANDRA_KEYSPACE}_{MOCK_TABLE_NAME}_{mock_time_value}"
    assert MOCK_TABLE_NAME in path

@patch("src.sinks.cassandra_sink.validate_checkpoint")
def test_write_stream_starts_query(mock_validate_checkpoint):
    """It should start a writeStream and register query in active list"""
    sink = CassandraSink(MOCK_CASSANDRA_KEYSPACE, checkpoint_root=MOCK_CHECKPOINT_ROOT)

    # Mock DataFrame and writer chain
    mock_df = MagicMock(spec=DataFrame)
    mock_writer = MagicMock()
    mock_query = MagicMock()
    mock_df.writeStream.option.return_value = mock_writer
    mock_writer.outputMode.return_value = mock_writer
    mock_writer.foreachBatch.return_value = mock_writer
    mock_writer.start.return_value = mock_query

    sink.write_stream(mock_df, table=MOCK_TABLE_NAME)

    # validate_checkpoint() must be called
    mock_validate_checkpoint.assert_called_once()
    mock_df.writeStream.option.assert_any_call("checkpointLocation", ANY)
    assert mock_query in sink._active_queries

def test_stop_active_queries(caplog):
    """It should stop active streaming queries gracefully"""
    caplog.set_level(logging.INFO)

    sink = CassandraSink(MOCK_CASSANDRA_KEYSPACE)

    active_query = MagicMock()
    active_query.isActive = True
    inactive_query = MagicMock()
    inactive_query.isActive = False

    sink._active_queries = [active_query, inactive_query]

    sink.stop()

    active_query.stop.assert_called_once()
    inactive_query.stop.assert_not_called()
    assert "Stopping streaming query" in caplog.text
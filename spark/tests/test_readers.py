# import os
# from unittest.mock import MagicMock
#
# import pytest
# from dotenv import load_dotenv
#
# from ..src.sources.kafka_source import KafkaStreamReader
#
# load_dotenv()
#
# KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
#
# def test_read_creates_dataframe(spark_session) -> None:
#     """
#     It should successfully create return a streaming DataFrame
#     """
#
#     # Use the 'rate' source as a dummy streaming source
#     df_stream = spark_session.readStream.format("rate").option("rowsPerSecond", 1).load()
#
#     # Patch the KafkaStreamReader to use our dummy streaming DataFrame
#     reader = KafkaStreamReader(spark_session, bootstrap_servers="dummy:9092")
#     reader._df = df_stream  # directly inject the stream
#
#     # Check that to_df() returns the stream
#     result_df = reader.to_df()
#     assert result_df.isStreaming  # streaming DataFrame property
#     assert result_df.columns == df_stream.columns
#
# def test_read_with_custom_starting_offsets() -> None:
#     """
#     It should successfully read with a custom startingOffsets value
#     """
#
#     mock_df = MagicMock(name="MockDataFrame")
#     mock_spark_session = MagicMock()
#     mock_format = MagicMock()
#     mock_format.option.return_value = mock_format
#     mock_format.load.return_value = mock_df
#     mock_spark_session.readStream.format.return_value = mock_format
#
#     reader = KafkaStreamReader(mock_spark_session, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     reader.read(KAFKA_TOPIC_NAME, starting_offsets="earliest")
#
#     mock_format.option.assert_any_call("startingOffsets", "earliest")
#
#     assert reader.to_df() == mock_df
#
# def test_read_failure_propagates_exception() -> None:
#     """
#     It should propagate an exception in the readStream chain.
#     """
#     mock_spark_session = MagicMock()
#     mock_spark_session.readStream.format.side_effect = RuntimeError("Kafka connection failed")
#
#     reader = KafkaStreamReader(spark=mock_spark_session, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     with pytest.raises(RuntimeError, match="Kafka connection failed"):
#         reader.read(KAFKA_TOPIC_NAME)
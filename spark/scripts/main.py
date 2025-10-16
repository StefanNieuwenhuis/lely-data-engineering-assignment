import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.config import (
    APP_NAME,
    SPARK_MASTER_SERVER,
    CASSANDRA_CONNECTION_HOST,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_NAME,
    CHECKPOINT_DIR
)
from src.utils import build_spark_session
from src.schemas import github_event_schema, github_event_field_map, output_schema, state_schema
from src.sources import KafkaStreamReader
from src.processors import GitHubEventParser, AveragePRIntervalProcessor
from src.sinks import CassandraSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(APP_NAME)

def main():
    # Initialize a new Spark Session
    spark_session = build_spark_session(APP_NAME, SPARK_MASTER_SERVER, CASSANDRA_CONNECTION_HOST)

    # Read Kafka Stream
    kafka_stream: DataFrame = KafkaStreamReader(spark_session, KAFKA_BOOTSTRAP_SERVERS).read(KAFKA_TOPIC_NAME).to_df()

    # Parse Kafka JSON payloads into a typed DataFrame.
    events_df = GitHubEventParser.parse(kafka_stream, github_event_schema, github_event_field_map)

    # Compute average time intervals between PRs
    avg_pr_df = AveragePRIntervalProcessor().run(events_df, state_schema, output_schema)

    # Upsert to Cassandra DB
    sink = CassandraSink("github_events", CHECKPOINT_DIR)
    sink \
        .write_stream(
            events_df.select(col("type"), col("created_at")),
            "event_counts_by_type"
        ) \
        .write_stream(avg_pr_df, "avg_pr_time") \
        .start()

if __name__ == "__main__":
    main()
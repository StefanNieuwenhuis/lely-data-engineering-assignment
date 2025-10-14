import logging

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

class KafkaStreamReader:
    """

    """
    def __init__(self, spark: SparkSession, bootstrap_servers: str):
        log.info("Intializing new Kafka Stream")
        self._spark = spark
        self._bootstrap_servers = bootstrap_servers
        self._df = None

    def read(self, topic: str, starting_offsets="latest") -> "KafkaStreamReader":
        log.info(f"Start reading from topic {topic} with starting offset: {starting_offsets}")

        self._df = (
            self._spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self._bootstrap_servers)
                .option("subscribe", topic)
                .option("startingOffsets", starting_offsets)
                .load()
        )
        return self

    def to_df(self) -> DataFrame:
        return self._df
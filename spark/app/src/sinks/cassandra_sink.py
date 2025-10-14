import logging
import os
from collections.abc import Callable
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

log = logging.getLogger(__name__)

class CassandraSink:
    """
    Generic Cassandra sink for Structured Streaming with support for foreachBatch writes
    and automatic checkpoint management.
    """
    def __init__(self, keyspace: str, checkpoint_root: str = "/opt/spark/checkpoints"):
        self._keyspace = keyspace
        self._checkpoint_root = checkpoint_root
        self._active_queries: List[StreamingQuery] = []

    def _upsert(self, batch_df: DataFrame, batch_id: int):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "github_events") \
            .option("table", "avg_pr_time") \
            .mode("append") \
            .save()

    def write_stream(self,
                     df: DataFrame,
                     table: str,
                     foreach_batch_fn: Callable[[DataFrame, int], None],
                     output_mode: str = "update",
                     checkpoint_subdir: str | None = None
    ) -> "CassandraSink":
        """
        Starts a structured streaming write to Cassandra using either
        foreachBatch or direct sink mode.

        :param df: source DataFrame
        :param table: Cassandra table name
        :param foreach_batch_fn: foreachBatch function for per-batch upsert logic
        :param output_mode: output mode ('append', 'update', or 'complete')
        :param checkpoint_subdir: optional subdirectory for checkpointing
        :return: CassandraSink instance (for chaining)
        """
        checkpoint_path = os.path.join(
            self._checkpoint_root,
            checkpoint_subdir or f"{self._keyspace}_{table}"
        )

        log.info(f"Starting write stream to Cassandra table={table}, checkpoint={checkpoint_path}")

        writer = df.writeStream \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode(output_mode) \
            .foreachBatch(lambda df, batch_id: self._upsert(df, batch_id))

        query = writer.start()
        self._active_queries.append(query)

        return self

    def start(self):
        """Wait for all active queries to terminate."""
        for q in self._active_queries:
            q.awaitTermination()

    def stop(self):
        """Gracefully stop all active queries."""
        for q in self._active_queries:
            if q.isActive:
                log.info(f"Stopping streaming query {q.name or q.id}")
                q.stop()
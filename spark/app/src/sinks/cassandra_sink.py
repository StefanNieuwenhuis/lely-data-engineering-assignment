import logging
import os
import time

from collections.abc import Callable
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from src.utils.validate_checkpoint import validate_checkpoint

log = logging.getLogger(__name__)


def upsert(batch_df: DataFrame, batch_id: int, table: str):
    if batch_df.isEmpty():
        log.info(f"[Batch {batch_id}] empty batch - skipping write.")
        return

    log.info(f"[Batch {batch_id}]: Writing {batch_df.count()} rows to table '{table}'")

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "github_events") \
        .option("table", table) \
        .mode("append") \
        .save()


class CassandraSink:
    """
    Generic Cassandra sink for Structured Streaming with support for foreachBatch writes
    and automatic checkpoint management.
    """
    def __init__(self, keyspace: str, checkpoint_root: str = "/opt/spark/checkpoints"):
        self._keyspace = keyspace
        self._checkpoint_root = checkpoint_root
        self._active_queries: List[StreamingQuery] = []

    def _construct_checkpoint_path(self, table: str) -> str:
        checkpoint_path = os.path.join(
            self._checkpoint_root,
            f"{self._keyspace}_{table}_{int(time.time())}"
        )

        log.info(f"Created checkpoint path: {checkpoint_path}")

        return checkpoint_path

    def write_stream(self,
                     df: DataFrame,
                     table: str,
                     foreach_batch_fn: Callable[[DataFrame, int], None] = None,
                     output_mode: str = "update",
    ) -> "CassandraSink":
        """Starts a structured streaming write to Cassandra using foreachBatch mode."""
        checkpoint_path = self._construct_checkpoint_path(table)

        # Check if the checkpoint path is not corrupted by e.g. the driver crashing mid-drive
        validate_checkpoint(checkpoint_path)
        log.info(f"Starting write stream to Cassandra table={table}, checkpoint={checkpoint_path}")

        writer = df.writeStream \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode(output_mode) \
            .foreachBatch(lambda data_frame, batch_id: upsert(data_frame, batch_id, table))

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
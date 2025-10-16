from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window, to_timestamp, to_date


class AggregateEventCountsProcessor:
    def run(self, df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
        """
        Aggregate event counts per type per time window
        """
        aggregate_event_counts_df = (
            df
            .withColumn("created_at", to_timestamp(col("created_at")))
            .withWatermark("created_at", "5 minutes")
            .withColumn("bucket_day", to_date("created_at"))
            .groupBy(
                col("type"),
                col("bucket_day"),
                window(col("created_at"), window_duration).alias("window")
            )
            .count()
            .select(
                col("type"),
                col("bucket_day"),
                col("window.start").alias("time_bucket"),
                col("count")
            )
        )

        return aggregate_event_counts_df
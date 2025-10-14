from pyspark.sql.types import StructType, TimestampType, IntegerType, DoubleType, StructField, StringType

state_schema = StructType([
    StructField("last_pull_request_ts", TimestampType()),
    StructField("count", IntegerType()),
    StructField("total_diff", DoubleType())
])

output_schema = StructType([
    StructField("repo_name", StringType()),
    StructField("avg_interval_seconds", DoubleType()),
    StructField("pr_count", DoubleType()),
    StructField("last_pull_request_ts", TimestampType()),
    StructField("updated_at", TimestampType())
])
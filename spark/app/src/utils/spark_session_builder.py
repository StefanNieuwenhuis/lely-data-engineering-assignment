from pyspark.sql import SparkSession

def build_spark_session(app_name: str, spark_master: str, cassandra_host: str) -> SparkSession:
    spark_session = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.cassandra.connection.host", cassandra_host)          # host or container name
        .config("spark.executor.cores", "2")                                # CPU cores per executor
        .config("spark.executor.memory", "4g")                              # memory per executor
        .config("spark.driver.memory", "2g")                                # driver memory
        .config("spark.cores.max", "4")                                     # max cores Spark can use
        .config("spark.sql.shuffle.partitions", "8")                        # partitions for aggregations/shuffles
        .config("spark.streaming.backpressure.enabled", "true")             # Spark adjusts ingestion rate automatically
        .config("spark.streaming.kafka.maxRatePerPartition", "5000")        # max messages per second per partition
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("INFO")

    return spark_session

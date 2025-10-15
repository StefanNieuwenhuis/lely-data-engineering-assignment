import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a single shared Spark Session for all tests
    The session runs in local mode - i.e. no cluster
    """
    spark_session = (
        SparkSession.builder
        .appName("GitHubEventStreamProcessor")
        .master("local[2]")                             # run with 2 threads
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop();
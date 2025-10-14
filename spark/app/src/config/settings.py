import os

from dotenv import load_dotenv

load_dotenv()

APP_NAME = os.getenv("APP_NAME")
SPARK_MASTER_SERVER = os.getenv("SPARK_MASTER_SERVER")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
CASSANDRA_CONNECTION_HOST = os.getenv("CASSANDRA_CONNECTION_HOST")
CHECKPOINT_DIR = "/opt/spark/checkpoints/github_events_avg_pr_time_v1"
import os

from dotenv import load_dotenv

load_dotenv()

CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
GITHUB_EVENTS_TYPES = os.getenv("GITHUB_EVENT_TYPES")
import os

from dotenv import load_dotenv

load_dotenv()

CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
import pytest
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from fastapi.testclient import TestClient

from main import app
from src.config import CASSANDRA_KEYSPACE
from src.database import CassandraConnection, get_cassandra_session

# Define mocking constants
TEST_CASSANDRA_KEYSPACE = f"{CASSANDRA_KEYSPACE}_test"
TEST_CASSANDRA_HOST = "cassandra"

@pytest.fixture(scope="session", autouse=True)
def setup_test_keyspace():
    """
    Creates (and later drops) a test Cassandra keyspace.
    """
    cluster = Cluster([TEST_CASSANDRA_HOST])
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {TEST_CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """)
    session.set_keyspace(TEST_CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory

    print(f"STEFAN, {session.keyspace}")

    # Create test table schema (mirror production)
    # See ./cassandra/init.cql for table definitions used in production.
    session.execute("""
            CREATE TABLE IF NOT EXISTS avg_pr_time (
                repo_name text PRIMARY KEY,
                pr_count int,
                avg_interval_seconds double,
                last_pull_request_ts timestamp,
                updated_at timestamp
            )
        """)

    yield # run all tests

    # cleanup
    session.execute(f"DROP KEYSPACE IF EXISTS {TEST_CASSANDRA_KEYSPACE}")
    cluster.shutdown()

@pytest.fixture(scope="function")
def test_session(monkeypatch):
    """
    Yields a Cassandra session bound to the test keyspace.
    Monkeypatches the app config so the singleton uses test data only.
    """

    CassandraConnection().shutdown() # reset CassandraConnection

    # Connect to test keyspace
    cluster = Cluster([TEST_CASSANDRA_HOST])
    session = cluster.connect(TEST_CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory

    # Patch singleton to use test session
    monkeypatch.setattr(CassandraConnection, "_cluster", cluster)
    monkeypatch.setattr(CassandraConnection, "_session", session)

    yield session

    # Clean up: close Cassandra Connection
    CassandraConnection.shutdown()

@pytest.fixture(scope="function")
def client(test_session):
    """Provides a FastAPI TestClient using a test Cassandra session"""

    # Define async dependency override
    async def override_get_cassandra_session():
        print(f"[TEST OVERRIDE] Using keyspace: {test_session.keyspace}")
        yield test_session

    # Apply the dependency override
    app.dependency_overrides[get_cassandra_session] = override_get_cassandra_session

    # Create the test client and yield it
    with TestClient(app) as c:
        yield c

    # Cleanup: remove overrides after test
    app.dependency_overrides.clear()
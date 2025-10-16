from typing import AsyncGenerator

from cassandra.cluster import Cluster, Session
from cassandra.query import dict_factory

from src.config import CASSANDRA_KEYSPACE


class CassandraConnection:
    """
    Singleton Cassandra Connection Manager
    """
    _cluster: Cluster | None = None
    _session: Session | None = None

    @classmethod
    def get_session(cls) -> Session:
        """Get a connected session (creates a new connection on the first call)"""
        if cls._session is None:
            cls._cluster = Cluster(["cassandra"])
            cls._session = cls._cluster.connect(CASSANDRA_KEYSPACE)
            cls._session.row_factory = dict_factory
        return cls._session

    @classmethod
    def shutdown(cls) -> None:
        """Shutdown Cassandra Connection"""
        if cls._cluster:
            cls._cluster.shutdown()
            cls._cluster = None
            cls._session = None

async def get_cassandra_session() -> AsyncGenerator[Session, None]:
    """
    Provides a Cassandra Connected Session to FastAPI endpoints.

    The connection is shared all requests (singleton).
    """

    session = CassandraConnection.get_session()
    yield session
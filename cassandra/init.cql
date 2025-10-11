CREATE KEYSPACE IF NOT EXISTS github_events
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
# Cassandra 

## Environment variables

| Name | Description| Required | Example value |
|------|------------|----------|---------------|
| `CASSANDRA_ENDPOINT_SNITCH` | Tells Cassandra how to locate nodes (SimpleSnitch for single node). | 	yes     | SimpleSnitch  |
| `CASSANDRA_START_RPC` | Enables legacy Thrift RPC (not needed for CQL). | optional | false |       

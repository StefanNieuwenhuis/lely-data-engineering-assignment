# Apache Kafka Connect

## Environment variables

| Name | Description| Required | Example value |
|------|------------|----------|---------------|
| `BOOTSTRAP_SERVERS` | Kafka broker address for Connect to use. |  yes | kafka:9092 |
| `REST_ADVERTISED_HOST_NAME` | Hostname used in the Connect REST API. |  yes | connect |
| `REST_PORT` | Port for the REST API. |  yes | 8083 |
| `GROUP_ID` | Connect cluster group ID (shared if multiple Connect nodes). |  yes | compose-connect-group |
| `CONFIG_STORAGE_TOPIC` | Topic for connector configs. |  yes | docker-connect-configs |
| `OFFSET_STORAGE_TOPIC` | Topic for connector offsets. |  yes | docker-connect-offsets |
| `STATUS_STORAGE_TOPIC` | Topic for connector statuses. |  yes | docker-connect-status |
| `CONFIG_STORAGE_REPLICATION_FACTOR` | Internal topic replication factor. |  yes | 1 |
| `OFFSET_STORAGE_REPLICATION_FACTOR` | Same for offsets. |  yes | 1 |
| `STATUS_STORAGE_REPLICATION_FACTOR` | Same for status. |  yes | 1 |
| `CONNECT_PLUGIN_PATH` | Where custom connectors/plugins are loaded from. |  yes | /opt/kafka/plugins,/opt/kafka/custom-plugins |
| `KEY_CONVERTER` | Converter for record keys. Often StringConverter. | optional | org.apache.kafka.connect.storage.StringConverter |
| `VALUE_CONVERTER` | Converter for record values. Usually JSON. | optional | org.apache.kafka.connect.json.JsonConverter |
| `CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY` | Allows connectors to override client configs (e.g., SSL, headers). | optional | All |

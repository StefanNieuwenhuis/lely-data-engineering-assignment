# Apache Kafka Connect

## Environment variables

| Name | Description| Required | Example value |
|------|------------|----------|---------------|
| `KAFKA_NODE_ID` | Unique ID for this Kafka process. In KRaft mode, used to identify the node in the quorum. |	yes |	1 |
| `KAFKA_PROCESS_ROLES` | Defines node role(s): broker, controller, or both. |	yes | broker,controller |
| `KAFKA_LISTENERS` | Defines network endpoints for communication. PLAINTEXT://:9092 (broker clients) and CONTROLLER://:9093 (KRaft controller). | yes | PLAINTEXT://:9092,CONTROLLER://:9093 |
| `KAFKA_ADVERTISED_LISTENERS` | What other containers should use to reach Kafka. |	yes | PLAINTEXT://kafka:9092 |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | Which listener is used by controllers. | yes | CONTROLLER |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Maps listener names to protocols. | yes | CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | List of controller node IDs and addresses (used by KRaft quorum). | yes | 1@localhost:9093 |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | Replication factor for internal topics (no effect if single node). | yes |	1 |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | Same as above, for transactional logs. | yes |	1 |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | Minimum in-sync replicas for transactional topics. | yes | 1 |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | Reduces startup time for consumer groups (set to 0 for dev). |	optional | 0 |
| `KAFKA_NUM_PARTITIONS` | Default number of partitions for new topics. | optional | 3 |

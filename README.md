# Lely Data Engineering Assignment
Monitor activities happening on GitHub

## Project Folder Structure

```shell
.
├── README.md
├── apache-kafka
│   └── README.md
└── docker-compose.yaml
```

## Services

| service                         | address[:port]       | docs                                 |
|---------------------------------|----------------------|--------------------------------------|
| Apache Kafka (inside container) | kafka:9092           | [README.md](kafka/README.md)         |
| Kafka Connect (custom)          | kafka-connect:8083   | [README.md](kafka-connect/README.md) |
| Cassandra                       | cassandra:9042       | [README.md](cassandra/README.md)     |
| Spark (master)                  | spark-master:8080    | [README.md](spark/README.md)         |
| Spark (history)                 | spark-history:18080  | [README.md](spark/README.md)         |



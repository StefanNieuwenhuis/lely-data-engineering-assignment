# Lely Data Engineering Assignment
Monitor activities happening on GitHub

## Project Folder Structure

```shell
.
├── README.md
├── cassandra
│   ├── README.md
│   └── init.cql
├── docker-compose.yaml
├── fastapi
│   ├── Dockerfile
│   ├── README.md
│   ├── app
│   │   ├── __init__.py
│   │   ├── main.py
│   │   └── src
│   │       ├── __init__.py
│   │       ├── config
│   │       │   ├── __init__.py
│   │       │   └── settings.py
│   │       ├── database
│   │       │   ├── __init__.py
│   │       │   └── cassandra.py
│   │       ├── models
│   │       │   ├── __init__.py
│   │       │   ├── agg_event_counts_model.py
│   │       │   └── avg_pr_interval_model.py
│   │       └── routers
│   │           ├── __init__.py
│   │           ├── agg_event_counts.py
│   │           └── avg_pr_interval.py
│   ├── requirements.txt
│   └── tests
│       ├── __init__.py
│       ├── conftest.py
│       ├── test_agg_event_counts.py
│       └── test_avg_pr_interval.py
├── kafka
│   └── README.md
├── kafka-connect
│   ├── Dockerfile
│   ├── README.md
│   ├── connectors-config
│   │   └── kafka-custom-http-connector.json
│   ├── custom-plugins
│   │   └── kafka-custom-http-connector
│   │       └── kafka-custom-http-connector-1.0.jar
│   ├── entrypoint.sh
│   ├── plugins.sh
│   └── scripts
└── spark
    ├── Dockerfile
    ├── README.md
    ├── app
    │   ├── __init__.py
    │   └── src
    │       ├── __init__.py
    │       ├── config
    │       │   ├── __init__.py
    │       │   └── settings.py
    │       ├── processors
    │       │   ├── __init__.py
    │       │   ├── __pycache__
    │       │   ├── aggregate_event_counts_processor.py
    │       │   ├── github_event_parser.py
    │       │   └── pr_interval_processor.py
    │       ├── schemas
    │       │   ├── __init__.py
    │       │   ├── avg_interval.py
    │       │   └── github_events.py
    │       ├── sinks
    │       │   ├── __init__.py
    │       │   └── cassandra_sink.py
    │       ├── sources
    │       │   ├── __init__.py
    │       │   └── kafka_source.py
    │       └── utils
    │           ├── __init__.py
    │           ├── spark_session_builder.py
    │           └── validate_checkpoint.py
    ├── conf
    │   └── spark-defaults.conf
    ├── entrypoint.sh
    ├── pyproject.toml
    ├── requirements.txt
    ├── scripts
    │   └── main.py
    └── tests
        ├── __init__.py
        ├── conftest.py
        ├── processors
        │   ├── test_aggregate_event_counts_processor.py
        │   ├── test_github_event_parser.py
        │   └── test_pr_interval_processor.py
        ├── sinks
        │   └── test_cassandra_sink.py
        └── sources
            └── test_kafka_source.py
```

## Services

| service                         | address[:port]       | docs                                 |
|---------------------------------|----------------------|--------------------------------------|
| Apache Kafka (inside container) | kafka:9092           | [README.md](kafka/README.md)         |
| Kafka Connect (custom)          | kafka-connect:8083   | [README.md](kafka-connect/README.md) |
| Cassandra                       | cassandra:9042       | [README.md](cassandra/README.md)     |
| Spark (master)                  | spark-master:8080    | [README.md](spark/README.md)         |
| Spark (history)                 | spark-history:18080  | [README.md](spark/README.md)         |



# Lely Data Engineering Assignment

Real-Time GitHub Event monitoring with Apache Kafka, PySpark, Cassandra, and FastAPI


## Table of Contents

- Architecture
- Prerequisites & Installation
- Running the Spark Jobs
- FastAPI endpoints
- Unit testing


## Architecture

This project contains a Kafka and PySpark streaming architecture. A [docker-compose](./docker-compose.yaml) file initializes (with all required dependencies):
- Kafka Cluster
- Spark Cluster
- Cassandra NoSQL database
- FastAPI Rest API

A custom-made-by-me Kafka HTTP-Connector polls the GitHub API every `[n]` milliseconds (see [.env.example](./.env.example)) and publishes the response body to a predefined Kafka topic.

A manually triggered Spark job reads from the Kafka Stream and computes the average time between pull requests, and the total number of events, grouped by event type. This job can be handled by orchestrators like Apache AirFlow, or DataBricks. It writes to Apache Cassandra, a noSql database which the FastAPI Rest API consumes.

![GitHub Event Stream Processor Architecture Diagram](./assets/ArchitectureDiagram.png)


## Prerequisites

This setup assumes an up-to-date instance of Docker Desktop with docker compose is installed. If not, please follow the installation instructions on the [offical Docker website](https://docs.docker.com/get-started/get-docker/).

> Please make sure you can run commands with `root` privileges on your machine!

## Installation

1. `git clone git@github.com:StefanNieuwenhuis/lely-data-engineering-assignment.git`
2. `cd lely-data-engineering-assignment`
3. `mv .env.example .env`
4. open `.env` and, when necessary, update values
5. `docker compose up -d`




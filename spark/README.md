# Apache Spark

## Environment variables

| Name | Description| Required | Example value                                           |
|------|------------|----------|---------------------------------------------------------|
| `SPARK_MODE` | Used in entrypoint to decide which role (master, worker, history) to run. | yes | master                                                  |
| `SPARK_MASTER` | URL for workers to connect to. |	yes | spark://spark-master:7077                               |
| `SPARK_WORKER_CORES` | How many CPU cores each worker uses. |	optional | 2                                                       |
| `SPARK_WORKER_MEMORY` | Memory per worker. |	optional | 2g                                                      |
| `SPARK_EVENT_LOG_ENABLED` | Enable Spark event logs (for history server). | yes | 	true                                                   |
| `SPARK_EVENT_LOG_DIR` | Directory to store logs. | yes | /opt/spark/spark-events                                 |
| `SPARK_HISTORY_OPTS` | Config for Spark history server (e.g., log dir). |	optional | -Dspark.history.fs.logDirectory=/opt/spark/spark-events |
| `SPARK_LOG_DIR` |	Where to store logs in container. |	optional | /opt/spark/logs                                         |
| `SPARK_SUBMIT_ARGS` |	Optional arguments for jobs (useful for debugging). | optional | --conf spark.executor.memory=1g                         |
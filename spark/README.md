# Apache Spark

## Mathematics behind "average time between pull requests"

### Defining the data

PR creation events for a repository $R$ is a **time series** of timestamps $T_1,T_2,T_3,\dots,T_n$, where each $T_i$ is the time a pull request is opened. We assume they are ordered such that $T_1<T_2<T_3<\dots<T_n$.

### Inter-arrival times

We define **inter-arrival times** - i.e. the time gaps between consecutive PRs as $\Delta_i=T_i-T_{i-1}$, where $i=2,3,\dots,n$.
So if PRs were opened at 10:00, 10:02, 10:08, 10:11, $\Delta_2 = $2 min, $\Delta_3 = $6 min, and $\Delta_4 = $3 min.

### Average inter-arrival times

The **empirical inter-arrival times** between PRs is: $\bar{\Delta} = \frac{1}{n-1}\sum\limits^n_{i=2}(T_i-T_{i-1})$, which simplifies to: $\bar{\Delta} = \frac{T_n-T_1}{n-1}$ - i.e. total elapsed time over the number of intervals.

**Interpretation**: Say that a repository has 11 PRs over the span of 10 days, then the average time between PRs is: $\frac{10\;days}{10} = 1$ day. 

### Streaming approximation

In streaming, the full history (i.e. $T_1, T_2, T_3, \dots, T_n$) is unavailable, so approximation using **event rate** over a time window $W$ of duration $|W|$ seconds is required: $AvgInterval(R,W)\approx\frac{|W|}{N_{R,W}}$, where $N_{R,W}$ is the number of PR Events for repository $R$ in window $W$.

If there are, for example, 10 PRs in 5 minutes, the average interval $\approx$ 30 seconds.
If there are 2 PRs in 10 minutes, the average interval $\approx$ 300 seconds.

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
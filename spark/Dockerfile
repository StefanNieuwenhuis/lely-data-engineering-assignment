FROM python:3.10-bullseye AS spark-base

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER=spark://spark-master:7077

# Create and set workdir
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download spark
RUN mkdir -p ${SPARK_HOME} \
    && curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o spark.tgz \
    && tar -xzf spark.tgz -C /opt/spark --strip-components=1 \
    && rm spark.tgz

# Install python dependencies from requirements.txt
COPY ./spark/requirements.txt .
RUN pip3 install -r requirements.txt

# Copy Spark Defaults Configuration
COPY ./spark/conf/spark-defaults.conf "$SPARK_HOME/conf"

# Make the binaries and scripts executable
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Set PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copy and set entrypoint
COPY ./spark/entrypoint.sh .
RUN chmod u+x /opt/spark/entrypoint.sh

# Define entrypoint and default command
ENTRYPOINT ["./entrypoint.sh"]
CMD [ "bash" ]

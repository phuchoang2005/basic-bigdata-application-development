# BASE: Jupyter SciPy Notebook
FROM jupyter/scipy-notebook:latest

# ---------------------------
# ARGIRONMENTS
# ---------------------------
ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.5.1
ARG COMMON_POOL2=2.12.0

ARG SPARK_KAFKA_CONNECTOR_VERSION=3.5.3

ARG SPARK_HOME=/usr/local/spark
ENV SPARK_JARS_DIR=$SPARK_HOME/jars

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/opt/conda/bin/python

# ---------------------------
# INSTALL JAVA + TOOLS
# ---------------------------

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ---------------------------
# INSTALL SPARK
# ---------------------------
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /usr/local/ && \
    mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# ---------------------------
# DOWNLOAD SPARK-KAFKA CONNECTORS
# ---------------------------

RUN set -eux; \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_KAFKA_CONNECTOR_VERSION}.jar" \
        -o "${SPARK_JARS_DIR}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_KAFKA_CONNECTOR_VERSION}.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
        -o "${SPARK_JARS_DIR}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMON_POOL2}/commons-pool2-${COMMON_POOL2}.jar" \
        -o "${SPARK_JARS_DIR}/commons-pool2-${COMMON_POOL2}.jar" && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        -o "${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"

    # ---------------------------
# SPARK CONFIG
# ---------------------------
RUN mkdir -p ${SPARK_HOME}/conf && \
    echo "export PYSPARK_PYTHON=${PYSPARK_PYTHON}" >> ${SPARK_HOME}/conf/spark-env.sh

# ---------------------------
# INSTALL PYTHON PACKAGES
# ---------------------------
USER $NB_UID

COPY ./requirements.txt /tmp/requirements.txt
RUN mamba run -n base pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8888

CMD ["start-notebook.sh"]

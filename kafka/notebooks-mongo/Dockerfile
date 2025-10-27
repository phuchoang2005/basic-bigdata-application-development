FROM ubuntu:22.04

USER root

# Cài Java + Python + các công cụ cơ bản
RUN apt update && \
    apt install -y python3 python3-pip openjdk-17-jdk curl && \
    apt clean && rm -rf /var/lib/apt/lists/*

# Cài Spark
ENV SPARK_VERSION=3.5.7
ENV HADOOP_VERSION=3
RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Cài Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install jupyter

WORKDIR /home/notebooks

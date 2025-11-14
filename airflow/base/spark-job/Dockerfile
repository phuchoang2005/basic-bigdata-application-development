# Dùng image Python cơ bản, nhẹ hơn Airflow
FROM python:3.11

# Cài Java 17 cho Spark
USER root

RUN apt-get update && apt-get install -y wget gnupg \
    && wget -qO /etc/apt/keyrings/adoptium.asc \
    https://packages.adoptium.net/artifactory/api/gpg/key/public \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] \
    https://packages.adoptium.net/artifactory/deb bookworm main" \
    > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y temurin-17-jre \
    && rm -rf /var/lib/apt/lists/*



RUN apt-get update && \
    apt-get install -y --no-install-recommends wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Cài đặt Spark Jars (cho Kafka và Postgres)
RUN mkdir -p /opt/spark/jars && cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Cài đặt thư viện Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

#Cài đặt PyTorch cho CPU
RUN pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cpu

# Tạo thư mục làm việc
WORKDIR /opt/project

FROM eclipse-temurin:11-jdk

# Install Python 3.11 & dependencies
# 1. Cài software-properties-common để thêm PPA
# 2. Thêm PPA deadsnakes (kho chứa các bản Python cũ/mới cho Ubuntu)
# 3. Cài Python 3.11, venv, distutils và dev (quan trọng để build thư viện)
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common wget curl \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv \
    python3.11-distutils \
    python3.11-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Tạo venv và upgrade pip
RUN python3.11 -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip setuptools wheel

ENV PATH="/opt/venv/bin:$PATH"
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

# Install Spark
ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENT_VERSION=3.5.1
ARG POSTGRES_VERSION=42.6.0
ARG COMMON_POOL2=2.12.0

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR $SPARK_HOME

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" --strip-components=1 \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p $SPARK_HOME/jars

WORKDIR $SPARK_HOME/jars

RUN \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka_${SCALA_VERSION}/${KAFKA_CLIENT_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_CLIENT_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMON_POOL2}/commons-pool2-${COMMON_POOL2}.jar"

WORKDIR /opt/spark-jobs

# ===== Install Python dependencies =====
COPY base/spark-job/requirements.txt /tmp/spark_requirements.txt
RUN pip install --no-cache-dir -r /tmp/spark_requirements.txt && \
    pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cpu

# ===== Patch kafka.vendor.six.moves lỗi thần thánh =====
# after pip install in venv
# ensure six and patch kafka package imports robustly using python
COPY base/spark-job/fix_kafka_vendor_six_moves.py /tmp/fix_kafka_vendor_six_moves.py
RUN pip install --no-cache-dir six || true && \
    python /tmp/fix_kafka_vendor_six_moves.py

CMD ["sleep", "infinity"]

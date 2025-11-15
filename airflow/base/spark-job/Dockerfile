FROM openjdk:21-jdk-slim

# Install Python & dependencies for pyspark jobs
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv wget curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Spark
# 1. Định nghĩa các phiên bản ở đầu file để dễ dàng thay đổi sau này (Single Source of Truth)
ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.13
ARG KAFKA_CLIENT_VERSION=3.5.1
ARG POSTGRES_VERSION=42.6.0

# 2. Thiết lập môi trường
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR $SPARK_HOME

# 3. Cài đặt Spark Core
# Sử dụng --strip-components=1 để giải nén trực tiếp vào /opt/spark (không tạo thêm thư mục con 'spark-3.5.2...')
RUN wget -q "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" --strip-components=1 \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p $SPARK_HOME/jars

# 4. Cài đặt Dependencies (JARs)
# Tách riêng bước này để tận dụng Docker Cache. Nếu bạn chỉ thay đổi version JAR, Docker sẽ không phải tải lại Spark Core.
WORKDIR $SPARK_HOME/jars

RUN \
    # Spark Kafka Connector
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \
    # Kafka Clients
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" && \
    # Lưu ý: kafka_2.13 thường không cần thiết cho Spark Client trừ khi bạn chạy embedded kafka, nhưng nếu cần thì giữ lại
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka_${SCALA_VERSION}/${KAFKA_CLIENT_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_CLIENT_VERSION}.jar" && \
    # PostgreSQL Driver
    wget -q "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar"

# Quay lại thư mục làm việc mặc định (nếu cần)
WORKDIR $SPARK_HOME

# ML dependencies
COPY base/spark-job/requirements.txt /tmp/spark_requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/spark_requirements.txt

WORKDIR /opt/spark-jobs
CMD ["sleep", "infinity"]

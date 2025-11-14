# ============================
# Dockerfile: Python 3.11 + PySpark + Java 21 + Kafka/Postgres
# ============================

FROM python:3.11

# 1️⃣ Cài các công cụ cơ bản
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl wget tar gnupg2 unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Thiết lập Java 21 từ kho Debian
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"


# 3️⃣ Tạo thư mục Spark JARs
RUN mkdir -p /opt/spark/jars

# 4️⃣ Tải các JAR tương thích PySpark 3.5.x + Kafka + Postgres
RUN cd /opt/spark/jars && \
    # Spark Kafka connector (Scala 2.13, Spark 3.5.2)
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.2/spark-sql-kafka-0-10_2.13-3.5.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/3.5.2/spark-token-provider-kafka-0-10_2.13-3.5.2.jar && \
    # Kafka clients mới nhất
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.13/3.5.1/kafka_2.13-3.5.1.jar && \
    # PostgreSQL driver
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# 5️⃣ Cài thư viện Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 6️⃣ Cài PyTorch CPU
RUN pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cpu

# 7️⃣ Thư mục làm việc
WORKDIR /opt/project

# 8️⃣ Kiểm tra phiên bản
RUN java -version
RUN python --version

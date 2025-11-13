# FROM apache/airflow:2.9.0

# USER root

# # Cài Java (cho Spark)
# RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH="${JAVA_HOME}/bin:${PATH}"

# # Copy file requirements trong chính thư mục base
# COPY base/requirements.txt /tmp/base_requirements.txt

# USER airflow
# RUN pip install -r /tmp/base_requirements.txt && \
#     # Fix lỗi kafka.vendor.six.moves không tồn tại trong kafka-python 2.0.1
#     sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
#     /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true

# # ================================
# # 🔧 Copy scripts và cấp quyền thực thi (chuẩn reproducible)
# # ================================
# USER root
# COPY projects/absa_streaming/scripts /opt/airflow/projects/absa_streaming/scripts
# RUN chmod +x /opt/airflow/projects/absa_streaming/scripts/*.sh
# USER airflow

# # Copy thư mục streamlit của project vào image
# COPY projects/absa_streaming/streamlit /opt/airflow/projects/absa_streaming/streamlit

# Use official Airflow base image
FROM apache/airflow:2.9.0

# ------------------------------------------------------------
# 🧰 System setup: Install Java 17 (for Spark)
# ------------------------------------------------------------
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ------------------------------------------------------------
# 🧩 Python dependencies (installed as airflow user)
# ------------------------------------------------------------
COPY base/requirements.txt /tmp/base_requirements.txt

USER airflow
RUN pip install -r /tmp/base_requirements.txt && \
    # Temporary workaround for kafka-python 2.0.1 bug
    sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
    /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true

# ------------------------------------------------------------
# 📁 Copy project files (scripts + streamlit)
# ------------------------------------------------------------
USER root
# Copy everything in one go to minimize Docker layers
COPY projects/absa_streaming /opt/airflow/projects/absa_streaming
RUN find /opt/airflow/projects/absa_streaming/scripts -type f -name "*.sh" -exec chmod +x {} \;

COPY models /opt/airflow/models

USER airflow

# ------------------------------------------------------------
# 🧰 Spark Setup: Install necessary jars (Kafka, PostgreSQL)
# ------------------------------------------------------------
USER root
RUN mkdir -p /opt/spark/jars && cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

USER airflow

# ------------------------------------------------------------
# ✅ Set useful defaults
# ------------------------------------------------------------
WORKDIR /opt/airflow
ENV PYTHONUNBUFFERED=1

# ------------------------------------------------------------
# 🧰 Expose the default Airflow port and Spark job runner
# ------------------------------------------------------------
EXPOSE 8080 4040

# Default command (Airflow entrypoint)
CMD ["bash"]

FROM apache/airflow:2.9.0

USER root

# Install only minimal system packages needed by Airflow tasks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget vim tini \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for Airflow (no ML, no Streamlit, no Java)
COPY base/airflow/requirements.txt /tmp/base_requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/base_requirements.txt && \
    # Fix Kafka python bug
    sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
    /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true

USER root
COPY projects/absa_streaming/scripts /opt/airflow/projects/absa_streaming/scripts
RUN chmod +x /opt/airflow/projects/absa_streaming/scripts/*.sh

USER airflow

WORKDIR /opt/airflow

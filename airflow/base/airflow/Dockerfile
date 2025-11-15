# Base image Airflow
FROM apache/airflow:2.9.0

# Copy file requirements của Airflow/Streamlit
COPY base/airflow/requirements.txt /tmp/requirements.txt

# Cài đặt với user airflow
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    # Workaround cho lỗi kafka-python 2.0.1
    sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
    /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true

USER root
# Cấp quyền cho scripts (nếu DAGs của bạn cần gọi)
RUN find /opt/airflow/projects/absa_streaming/scripts -type f -name "*.sh" -exec chmod +x {} \; || true
USER airflow

WORKDIR /opt/airflow

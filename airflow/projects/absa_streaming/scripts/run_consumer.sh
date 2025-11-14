#!/bin/bash
# ==========================================
# Script: run_consumer.sh
# Chức năng:
#   - Khởi động Spark Structured Streaming Consumer
#   - Đọc dữ liệu từ Kafka, chạy inference model ABSA
#   - Ghi kết quả vào PostgreSQL
# ==========================================

set -e

echo "[Consumer] Starting Spark Structured Streaming job..."

spark-submit \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.6.1.jar,/opt/spark/jars/postgresql-42.6.0.jar \
  /opt/airflow/projects/scripts/consumer_postgres_streaming.py

status=$?
if [ $status -eq 0 ]; then
  echo "[Consumer] Completed successfully."
else
  echo "[Consumer] Failed with exit code $status."
fi

exit $status

docker compose run --rm \
  spark-job-builder \
  spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.2,org.apache.kafka:kafka-clients:3.5.1,org.apache.commons:commons-pool2:2.12.0,org.postgresql:postgresql:42.6.0 \
    /opt/spark-jobs/projects/scripts/consumer_postgres_streaming.py

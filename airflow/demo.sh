docker compose run --rm \
    spark-job-builder \
    spark-submit \
        --master local[*] \
        /opt/spark-jobs/projects/scripts/producer.py

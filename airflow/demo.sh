docker compose run --rm \
    -v ${AIRFLOW_PROJ_DIR:-.}/projects/absa_streaming:/opt/projects \
    -v ${AIRFLOW_PROJ_DIR:-.}/models:/opt/project/models \
    -v ${AIRFLOW_PROJ_DIR:-.}/checkpoints:/opt/airflow/checkpoints \
    spark-job-builder \
    spark-submit \
        --master local[*] \
        --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.6.1.jar,/opt/spark/jars/postgresql-42.6.0.jar \
        /opt/projects/scripts/producer.py

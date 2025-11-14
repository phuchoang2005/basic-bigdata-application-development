docker compose run --rm \
    -v ${AIRFLOW_PROJ_DIR:-.}/projects/absa_streaming:/opt/airflow/projects \
    -v ${AIRFLOW_PROJ_DIR:-.}/models:/opt/airflow/models \
    -v ${AIRFLOW_PROJ_DIR:-.}/checkpoints:/opt/airflow/checkpoints \
    spark-job-builder \
    spark-submit \
        --master local[*] \
        --jars /opt/spark/jars/spark-sql-kafka-0-10_2.13-3.5.2.jar,/opt/spark/jars/postgresql-42.6.0.jar \
        /opt/airflow/projects/scripts/test.py


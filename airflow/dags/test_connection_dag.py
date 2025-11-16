# /opt/airflow/dags/dag_test_kafka.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="test_kafka_connection",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    produce = BashOperator(
        task_id="deploy_producer",
        # Giả định script này tự động dừng sau 45m hoặc 'timeout' sẽ ngắt nó
        bash_command="""
                chmod +x /opt/airflow/projects/scripts/run_test_producer.sh
                timeout 45m /opt/airflow/projects/scripts/run_test_producer.sh
            """,
        retries=3,
        execution_timeout=timedelta(minutes=50),
    )
    import os

    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    AIRFLOW_PROJ_DIR = os.environ.get("AIRFLOW_PROJ_DIR", os.path.abspath("."))
    CHECKPOINT_PATH = f"{AIRFLOW_PROJ_DIR}/checkpoints"
    PROJECT_PATH = f"{AIRFLOW_PROJ_DIR}/projects/absa_streaming"

    consume = deploy_consumer_spark = DockerOperator(
        task_id="deploy_consumer_spark",
        image="spark-cnn-job:latest",
        command="python /opt/spark-jobs/projects/scripts/test_consumer.py",
        network_mode="airflow_absa_network",
        auto_remove=True,
        mounts=[
            Mount(
                source=PROJECT_PATH,
                target="/opt/spark-jobs/projects",
                type="bind",
            ),
            Mount(
                source=CHECKPOINT_PATH,
                target="/opt/spark-jobs/checkpoints",
                type="bind",
            ),
        ],
        docker_url="unix://var/run/docker.sock",
        retries=5,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=50),
        mount_tmp_dir=False,
    )

    [produce, consume]

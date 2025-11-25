# ===========================================
# DAG: ABSA Streaming Lifecycle Orchestration (1-Hour Demo)
# ĐÃ CẬP NHẬT CHO KIẾN TRÚC DOCKEROPERATOR
# ===========================================
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

AIRFLOW_PROJ_DIR = os.environ.get("AIRFLOW_PROJ_DIR", os.path.abspath("."))
CHECKPOINT_PATH = f"{AIRFLOW_PROJ_DIR}/checkpoints"
PROJECT_PATH = f"{AIRFLOW_PROJ_DIR}/projects/absa_streaming"
MODEL_PATH = f"{AIRFLOW_PROJ_DIR}/models"

# === Default parameters ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# === Biến cục bộ (Host path) ===
# Cần map thư mục này vào DockerOperator
# === DAG definition ===
with DAG(
    dag_id="absa_streaming_lifecycle_demo",
    default_args=default_args,
    description="Orchestrate Kafka–Spark–PostgreSQL streaming lifecycle",
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=55),
    tags=["absa", "streaming", "kafka", "spark", "docker"],
) as dag:
    # === 1️⃣ Khởi động Producer ===
    # Tác vụ này vẫn chạy OK vì producer là script python nhẹ
    # và image 'airflow-base-local' có kafka-python.
    deploy_producer = DockerOperator(
        task_id="deploy_producer_spark",
        image="spark-cnn-job:latest",
        command="python /opt/spark-jobs/projects/scripts/producer.py",
        network_mode="airflow_absa_network",
        auto_remove=True,
        mounts=[
            Mount(
                source=PROJECT_PATH,
                target="/opt/spark-jobs/projects",
                type="bind",
            ),
        ],
        docker_url="unix://var/run/docker.sock",
        retries=5,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=50),
        mount_tmp_dir=False,
    )


    # === 2️⃣ Khởi động Consumer (Spark Job) bằng DockerOperator ===
    # ĐÂY LÀ THAY ĐỔI QUAN TRỌNG NHẤT

    deploy_consumer_spark = DockerOperator(
        task_id="deploy_consumer_spark",
        image="spark-cnn-job:latest",
        command="spark-submit \
            --master local[*] \
            /opt/spark-jobs/projects/scripts/consumer_postgres_streaming.py",
        network_mode="airflow_absa_network",
        auto_remove=True,
        mounts=[
            Mount(source=MODEL_PATH, target="/opt/spark-jobs/models", type="bind"),
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
    cleanup_checkpoints = BashOperator(
        task_id="cleanup_checkpoints",
        # SỬA LẠI LỆNH BASH:
        bash_command=(
            "echo '[Cleanup] Removing old checkpoint...'; "
            # Xóa đường dẫn BÊN TRONG container mà Spark đã ghi
            "rm -rf /opt/airflow/checkpoints/absa_streaming_checkpoint || true; "
            "echo '[Cleanup] Done.'"
        ),
    )

    [deploy_producer, deploy_consumer_spark] >> cleanup_checkpoints

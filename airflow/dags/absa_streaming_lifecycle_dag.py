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
    description="Orchestrate Kafka–Spark–PostgreSQL streaming lifecycle (1-Hour Demo)",
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=55),
    tags=["absa", "streaming", "kafka", "spark", "docker"],
) as dag:
    # === 1️⃣ Khởi động Producer ===
    # Tác vụ này vẫn chạy OK vì producer là script python nhẹ
    # và image 'airflow-base-local' có kafka-python.
    deploy_producer = BashOperator(
        task_id="deploy_producer",
        # Giả định script này tự động dừng sau 45m hoặc 'timeout' sẽ ngắt nó
        bash_command=f'bash -c "timeout 45m /opt/airflow/projects/scripts/run_producer.sh"',
        retries=3,
        execution_timeout=timedelta(minutes=50),
    )

    # === 2️⃣ Khởi động Consumer (Spark Job) bằng DockerOperator ===
    # ĐÂY LÀ THAY ĐỔI QUAN TRỌNG NHẤT
    deploy_consumer_spark = DockerOperator(
        task_id="deploy_consumer_spark",
        # 1. Tên image Spark CNN bạn đã build
        image="spark-cnn-job:latest",
        # 2. Lệnh spark-submit (giả định)
        # Thay thế bằng lệnh chính xác của bạn trong script run_consumer.sh
        command="spark-submit \
        --jars /opt/spark/jars/spark-sql-kafka-0-10_2.13-3.5.2.jar,/opt/spark/jars/postgresql-42.6.0.jar \
        /opt/spark-jobs/projects/scripts/consumer_postgres_streaming.py",
        # 3. Kết nối vào network của docker-compose
        # THAY 'your-project_default' bằng tên network thật (chạy 'docker network ls')
        network_mode="absa_network",
        # 4. Tự động xóa container sau khi chạy xong
        auto_remove=True,
        # 5. Cần thiết để Airflow gọi Docker
        docker_url="unix://var/run/docker.sock",
        # 7. Giữ lại các cài đặt timeout và retry
        retries=5,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=50),  # Tác vụ sẽ tự thất bại sau 50p
    )

    # === 3️⃣ Giám sát checkpoint ===
    # Tác vụ này đã được XÓA BỎ.
    # DockerOperator đã thay thế vai trò giám sát.

    # === 4️⃣ Dọn dẹp checkpoint ===
    # Tác vụ này bây giờ chạy sau khi CẢ HAI producer và consumer THÀNH CÔNG
    # === 4️⃣ Dọn dẹp checkpoint ===
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

    # === Task dependency (Flow MỚI) ===
    # 1. Producer và Consumer (Spark) chạy song song
    # 2. Nếu CẢ HAI cùng thành công, chạy cleanup
    [deploy_producer, deploy_consumer_spark] >> cleanup_checkpoints

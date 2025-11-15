# Cập nhật hệ thống & Tối ưu hóa Docker

## 1. Tóm tắt thay đổi (Changelog)

### Phân tách Service (Docker Profiles)

Để tối ưu hóa tài nguyên, các service đã được chia vào các profile riêng biệt và không khởi động mặc định:

- **Profile `spark`**: Chứa Spark/ML service.
- **Profile `kafka`**: Chứa Kafka & Zookeeper (dành cho streaming).
- **Profile `ui`**: Chứa Streamlit dashboard.
- **Profile `extras`**: Chứa Flower.

### Cấu hình & Hiệu năng (Development)

- **Airflow Tuning**:
  - Giảm `PARALLELISM=4` và `WORKER_CONCURRENCY=1` để tiết kiệm CPU/RAM.
  - `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` để giảm tải Database.
- **Resource Limits**: Thiết lập giới hạn `cpus` và `mem_limit` cho các service nặng (Redis, Postgres, Airflow worker).
- **Image Optimization**: Streamlit chuyển sang dùng `python:3.10-slim` kết hợp mount volume, thay thế cho image `airflow-base` nặng nề.
- **Docker Behavior**:
  - Sử dụng `restart: unless-stopped`.
  - Tinh chỉnh `healthchecks` (giảm `start_period`/`retries`) để khởi động nhanh hơn trong môi trường dev.
  - `airflow-init` được tối ưu hóa việc mount file.

---

## 2. Hướng dẫn sử dụng (Usage)

Dưới đây là các lệnh `docker compose` thường dùng:

### Khởi động Core Airflow (Minimal)

Chỉ chạy các thành phần thiết yếu (Postgres, Redis, Airflow services):

```bash
docker compose --profile Infrastructure up -d

docker compose --profile Database -d

docker compose --profile kafka up -d

docker compose --profile spark up -d

docker compose --profile ui up -d

docker compose --profile extras up -d
```

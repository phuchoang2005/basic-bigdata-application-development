# =========================
# Dockerfile for Flask + Kafka streaming
# =========================
FROM python:3.11-slim

# # Cài các gói cần thiết cho OpenCV và hệ thống
# RUN apt-get update && apt-get install -y \
#     libgl1-mesa-glx \
#     ffmpeg

# Tạo thư mục làm việc
WORKDIR /home/code

# Copy requirements và cài dependencies
COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

# Copy toàn bộ code vào container
COPY . .

# Mở cổng Flask
EXPOSE 5000

# Chạy Flask (sẽ đọc producer/consumer bên trong)
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]

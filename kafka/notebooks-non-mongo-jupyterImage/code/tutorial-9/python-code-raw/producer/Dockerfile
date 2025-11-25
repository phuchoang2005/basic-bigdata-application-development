FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python opencv-python-headless numpy

COPY producer.py .

CMD ["python", "producer.py"]

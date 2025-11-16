# /opt/spark-jobs/projects/scripts/test_producer.py
import time

from kafka import KafkaProducer

# Kafka broker (dùng hostname container trong docker-compose)
KAFKA_BROKER = "kafka:9092"
TOPIC = "test_topic"

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

for i in range(10):
    message = f"Hello {i}".encode("utf-8")
    producer.send(TOPIC, message)
    print(f"Sent: {message}")
    time.sleep(5)

producer.flush()
print("All messages sent!")

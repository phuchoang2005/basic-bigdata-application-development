# /opt/spark-jobs/projects/scripts/test_consumer.py
from kafka import KafkaConsumer

KAFKA_BROKER = "kafka:9092"
TOPIC = "test_topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="test-group",
)

print("Consumer started...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
    if message.value.decode("utf-8") == "Hello 9":
        exit(0)

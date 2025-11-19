from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "user-activity",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="activity-consumers"
)

print("Consumer started...")

for msg in consumer:
    print("Received:", msg.value)
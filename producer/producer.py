from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = ["samuel", "julia", "kofi", "arjun", "emma"]

def generate_event():
    return {
        "user": random.choice(users),
        "action": random.choice(["login", "logout", "purchase", "click"]),
        "timestamp": time.time()
    }

if __name__ == "__main__":
    print("Starting producer…")
    while True:
        event = generate_event()
        producer.send("user-activity", event)
        print("Sent:", event)
        time.sleep(1)
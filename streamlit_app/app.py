import streamlit as st
from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time

st.set_page_config(page_title="Real-Time Kafka Dashboard", layout="wide")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

st.title("📡 Real-Time User Activity Tracking (Kafka + Streamlit)")

# --- PRODUCER UI ---
st.subheader("Send User Events")

user = st.selectbox("User", ["samuel", "julia", "kofi", "arjun", "emma"])
action = st.selectbox("Action", ["login", "logout", "click", "purchase"])

if st.button("Send Event"):
    event = {"user": user, "action": action, "timestamp": time.time()}
    producer.send("user-activity", event)
    st.success(f"Event sent: {event}")

# --- REAL-TIME CONSUMER BOX ---
st.subheader("Live Event Stream")
placeholder = st.empty()

messages = []

def consume_messages():
    consumer = KafkaConsumer(
        "user-activity",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="streamlit-dashboard"
    )
    for msg in consumer:
        messages.append(msg.value)

thread = threading.Thread(target=consume_messages, daemon=True)
thread.start()

while True:
    placeholder.json(messages[-10:])  # show last 10 events
    time.sleep(1)
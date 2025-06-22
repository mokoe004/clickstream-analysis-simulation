import os
import json
import time
from typing import List, Dict
from kafka import KafkaProducer

def send_events_to_kafka(events: List[Dict]):
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "clickstream")

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10
    )

    for event in events:
        producer.send(topic, value=event)
        print(f"📤 Gesendet: {event['event_id']}")
        time.sleep(0.05)

    producer.flush()
    producer.close()

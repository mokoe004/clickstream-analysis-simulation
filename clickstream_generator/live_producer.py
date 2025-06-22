import time
import uuid
import random
from datetime import datetime, timezone

from clickstream_generator.data_generator import generate_session_events
from clickstream_generator.kafka_producer import send_events_to_kafka

def run_live_stream(sessions_per_minute=5):
    print("🚀 Starte Echtzeit-Datenstream zu Kafka...")

    while True:
        now = datetime.now(timezone.utc)
        print(f"🕒 Generiere Events für Timestamp: {now.isoformat()}")

        events = []
        for i in range(sessions_per_minute):
            session_id = f"s_{uuid.uuid4().hex[:8]}"
            user_id = f"u_{random.randint(1000, 9999)}"
            try:
                session_events = generate_session_events(session_id, user_id, now)
                events.extend(session_events)
            except Exception as e:
                print(f"❌ Fehler beim Generieren von Events: {e}")

        try:
            send_events_to_kafka(events)
            print(f"📤 {len(events)} Events an Kafka gesendet.")
        except Exception as e:
            print(f"❌ Fehler beim Senden an Kafka: {e}")

        print("⏳ Warte 10 Sekunden...\n")
        time.sleep(10)

if __name__ == "__main__":
    run_live_stream(sessions_per_minute=5)

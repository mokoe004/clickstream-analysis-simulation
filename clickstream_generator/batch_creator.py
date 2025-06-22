from datetime import datetime, timedelta
from pathlib import Path
import uuid
import random

from clickstream_generator.data_generator import generate_session_events
from clickstream_generator.file_writer import save_events_to_csv

def generate_batch_csv(days=90, sessions_per_day=100):
    output_dir = Path("clickstream_logs")
    output_dir.mkdir(exist_ok=True)

    start_date = datetime.utcnow() - timedelta(days=days)

    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        filepath = output_dir / f"clickstream_{date_str}.csv"

        events = []
        for _ in range(sessions_per_day):
            session_id = f"s_{uuid.uuid4().hex[:8]}"
            user_id = f"u_{random.randint(1000, 9999)}"
            session_time = date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            events.extend(generate_session_events(session_id, user_id, session_time))

        save_events_to_csv(events, filepath)
        print(f"✅ {len(events)} Events für {date_str} gespeichert")

if __name__ == "__main__":
    generate_batch_csv(days=90, sessions_per_day=100)

import json
import os
import random
import uuid
import time
from datetime import datetime, timedelta
from pathlib import Path
from kafka import KafkaProducer

# ------------------------------------
# Produktdaten (9 Produkte)
# ------------------------------------
PRODUCTS = [
    {"product_id": "vacuum_01", "name": "Staubi AirLite 2000", "category": "handstaubsauger"},
    {"product_id": "vacuum_02", "name": "Staubi CleanMax Turbo", "category": "bodenstaubsauger"},
    {"product_id": "vacuum_03", "name": "Staubi RoboSmart X5", "category": "saugroboter"},
    {"product_id": "vacuum_04", "name": "Staubi ProWet Dry 300", "category": "nass_trockensauger"},
    {"product_id": "vacuum_05", "name": "Staubi SilentPower Eco", "category": "bodenstaubsauger"},
    {"product_id": "vacuum_06", "name": "Staubi Cordless Flex", "category": "akkusauger"},
    {"product_id": "vacuum_07", "name": "Staubi RoboMini S1", "category": "saugroboter"},
    {"product_id": "vacuum_08", "name": "Staubi PetPower Ultra", "category": "tierstaubsauger"},
    {"product_id": "vacuum_09", "name": "Staubi AllRounder Plus", "category": "kombigerät"},
]

PAGES = ["homepage", "product_list", "product_detail", "cart", "checkout"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
OS_LIST = ["Windows", "macOS", "iOS", "Android"]
ACTIONS = ["view", "click", "scroll", "add_to_cart", "purchase"]
UTM_SOURCES = ["google", "facebook", "newsletter", "instagram"]
CITIES = [("Berlin", "Berlin"), ("München", "Bayern"), ("Hamburg", "Hamburg"), ("Köln", "NRW")]

NUM_USERS = 200
SESSIONS_PER_DAY = 100
DAYS = 90

output_dir = Path("clickstream_logs")
output_dir.mkdir(exist_ok=True)

# ------------------------------------
# Session / Event Generator
# ------------------------------------
def random_session_events(session_id, user_id, base_time):
    events = []
    num_steps = random.choices([2, 3, 4, 5], weights=[0.1, 0.2, 0.4, 0.3])[0]
    product = random.choice(PRODUCTS)
    ab_variant = random.choice(["A", "B"])
    referrer = f"https://{random.choice(UTM_SOURCES)}.com"
    campaign = random.choice(["summer_sale", "retargeting", "product_launch", "remarketing"])
    geo_city, geo_region = random.choice(CITIES)
    device = random.choice(DEVICES)
    os = random.choice(OS_LIST)
    browser = random.choice(BROWSERS)
    language = random.choice(["de-DE", "en-US"])
    is_logged_in = random.random() < 0.5

    current_time = base_time
    scroll = 0
    for step in range(num_steps):
        action = ACTIONS[min(step, len(ACTIONS) - 1)]
        page = PAGES[min(step, len(PAGES) - 1)]
        scroll = min(100, scroll + random.randint(10, 40))
        duration = round(random.uniform(2.0, 30.0), 2)
        current_time += timedelta(seconds=random.randint(5, 60))

        # Produktdetailseitenlogik
        product_id = product["product_id"] if page == "product_detail" else None
        page_url = f"/products/{product['product_id']}" if page == "product_detail" else f"/{page}"

        event = {
            "event_id": f"evt_{uuid.uuid4()}",
            "timestamp": current_time.isoformat(),
            "user_id": user_id,
            "session_id": session_id,
            "page": page,
            "page_url": page_url,
            "product_id": product_id,
            "category": product["category"] if product_id else None,
            "action": action,
            "element_id": f"btn_{action}" if action in ["click", "add_to_cart", "purchase"] else None,
            "position_x": random.randint(0, 1920),
            "position_y": random.randint(0, 1080),
            "referrer": referrer,
            "utm_source": referrer.split("//")[1].split(".")[0],
            "utm_medium": "cpc",
            "utm_campaign": campaign,
            "device_type": device,
            "os": os,
            "browser": browser,
            "language": language,
            "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
            "geo_country": "DE",
            "geo_region": geo_region,
            "geo_city": geo_city,
            "page_duration": duration,
            "scroll_depth_percent": scroll,
            "ab_test_variant": ab_variant,
            "is_logged_in": is_logged_in
        }

        events.append(event)
    return events

# ------------------------------------
# Log-Erstellung für 3 Monate
# ------------------------------------
start_date = datetime.utcnow() - timedelta(days=DAYS)

for day_offset in range(DAYS):
    current_day = start_date + timedelta(days=day_offset)
    log_file = output_dir / f"clickstream_{current_day.strftime('%Y-%m-%d')}.json"

    daily_events = []
    for _ in range(SESSIONS_PER_DAY):
        user_id = f"u{random.randint(1000, 9999)}"
        session_id = f"s{uuid.uuid4().hex[:8]}"
        session_time = current_day + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
        events = random_session_events(session_id, user_id, session_time)
        daily_events.extend(events)

    with log_file.open("w", encoding="utf-8") as f:
        for event in daily_events:
            f.write(json.dumps(event) + "\n")

print(f"✅ Clickstream Logs für {DAYS} Tage in '{output_dir.absolute()}' erstellt.")



# DOCKER COMMANDS

# docker run -d --name=kafka -p 9092:9092 apache/Kafka

# docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server :9092 --topic clickstream

# docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server :9092 --topic clickstream --from-beginning


def main():
    # 1) Docker-Kafka localhost:9092
    broker = os.getenv("KAFKA_BROKER", "localhost:9092") # Broker vorher starten
    topic  = os.getenv("KAFKA_TOPIC", "clickstream") # Topic vorher erstelen

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10
    )

    days = 90
    sessions_per_day = 100
    start_date = datetime.utcnow() - timedelta(days=days)

    topic_limiter = 10
    topic_counter = 0

    if topic_counter < topic_limiter:
        for d in range(days):
            day = start_date + timedelta(days=d)
            for _ in range(sessions_per_day):
                user_id    = f"u{random.randint(1000,9999)}"
                session_id = f"s{uuid.uuid4().hex[:8]}"
                base_time  = day + timedelta(
                    hours=random.randint(0,23),
                    minutes=random.randint(0,59)
                )
                events = random_session_events(session_id, user_id, base_time)
                for evt in events:
                    producer.send(topic, value=evt)
                    topic_counter + 1
                    # nur, falls Du sie nicht zu schnell feuern willst:
                    # time.sleep(0.01)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()

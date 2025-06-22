import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict

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

def generate_session_events(session_id: str, user_id: str, base_time: datetime) -> List[Dict]:
    events = []
    steps = random.randint(2, 5)
    product = random.choice(PRODUCTS)
    ab_variant = random.choice(["A", "B"])
    referrer_source = random.choice(UTM_SOURCES)
    referrer = f"https://{referrer_source}.com"
    campaign = random.choice(["summer_sale", "retargeting", "product_launch", "remarketing"])
    city, region = random.choice(CITIES)
    device = random.choice(DEVICES)
    os_name = random.choice(OS_LIST)
    browser = random.choice(BROWSERS)
    language = random.choice(["de-DE", "en-US"])
    is_logged_in = random.random() < 0.5

    current_time = base_time
    scroll_depth = 0

    for step in range(steps):
        action = ACTIONS[min(step, len(ACTIONS) - 1)]
        page = PAGES[min(step, len(PAGES) - 1)]
        scroll_depth = min(100, scroll_depth + random.randint(10, 40))
        duration = round(random.uniform(2.0, 30.0), 2)
        current_time += timedelta(seconds=random.randint(5, 60))

        product_id = product["product_id"] if page == "product_detail" or action in ["add_to_cart", "purchase"] else None
        page_url = f"/products/{product['product_id']}" if page == "product_detail" else f"/{page}"

        events.append({
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
            "utm_source": referrer_source,
            "utm_medium": "cpc",
            "utm_campaign": campaign,
            "device_type": device,
            "os": os_name,
            "browser": browser,
            "language": language,
            "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
            "geo_country": "DE",
            "geo_region": region,
            "geo_city": city,
            "page_duration": duration,
            "scroll_depth_percent": scroll_depth,
            "ab_test_variant": ab_variant,
            "is_logged_in": is_logged_in
        })

    return events

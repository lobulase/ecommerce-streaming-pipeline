"""
E-Commerce Order Event Generator
Simulates a real-time stream of order lifecycle events (placed, payment_confirmed,
shipped, delivered) and publishes them to a Pub/Sub topic.

Usage:
    python generator/publish_events.py --rate 2 --duration 300
    (publishes ~2 events/sec for 300 seconds)
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from google.cloud import pubsub_v1

PROJECT_ID = "f1-pipeline-497820"
TOPIC_NAME = "ecommerce-orders"

fake = Faker()

PRODUCT_CATEGORIES = [
    "Electronics", "Home & Kitchen", "Sports & Outdoors", "Books",
    "Clothing", "Toys", "Beauty", "Automotive"
]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card"]
EVENT_TYPES = ["order_placed", "payment_confirmed", "shipped", "delivered"]
EVENT_WEIGHTS = [0.45, 0.30, 0.15, 0.10]


def generate_event(order_id: str = None, event_type: str = None) -> dict:
    """Generate a single realistic order event."""
    now = datetime.now(timezone.utc)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 500.0), 2)

    return {
        "order_id": order_id or str(uuid.uuid4()),
        "event_type": event_type or random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0],
        "event_timestamp": now.isoformat(),
        "customer_id": f"cust_{random.randint(1000, 9999)}",
        "product_id": f"prod_{random.randint(100, 999)}",
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "quantity": quantity,
        "unit_price": unit_price,
        "order_total": round(quantity * unit_price, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_country": fake.country_code(),
    }


def publish_loop(rate: float, duration: int):
    """Publish events at approximately `rate` events/second for `duration` seconds."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    interval = 1.0 / rate
    end_time = time.time() + duration
    published_count = 0
    futures = []

    print(f"Publishing to {topic_path}")
    print(f"Rate: {rate}/sec, Duration: {duration}s")

    while time.time() < end_time:
        event = generate_event()
        data = json.dumps(event).encode("utf-8")
        future = publisher.publish(topic_path, data)
        futures.append(future)
        published_count += 1

        if published_count % 20 == 0:
            print(f"  Published {published_count} events...")

        time.sleep(interval)

    for future in futures:
        future.result()

    print(f"Done. Published {published_count} events total.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish simulated e-commerce order events")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=300, help="How long to run, in seconds")
    args = parser.parse_args()

    publish_loop(rate=args.rate, duration=args.duration)
"""
E-Commerce Order Event Subscriber
Pulls order events from the Pub/Sub subscription and streams them into BigQuery
using the streaming insert API (tabledata.insertAll).

Usage:
    python subscriber/consume_events.py
    (runs until interrupted with Ctrl+C)
"""
import json
from datetime import datetime, timezone

from google.cloud import bigquery, pubsub_v1

PROJECT_ID = "f1-pipeline-497820"
SUBSCRIPTION_NAME = "ecommerce-orders-sub"
DATASET_NAME = "ecommerce_streaming"
TABLE_NAME = "orders_raw"

bq_client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

BATCH_SIZE = 10
batch = []


def flush_batch():
    """Insert the current batch of rows into BigQuery."""
    global batch
    if not batch:
        return

    errors = bq_client.insert_rows_json(table_ref, batch)
    if errors:
        print(f"  BigQuery insert errors: {errors}")
    else:
        print(f"  Flushed {len(batch)} rows to BigQuery")
    batch = []


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Process a single Pub/Sub message: parse, enrich, and queue for BigQuery insert."""
    global batch
    try:
        event = json.loads(message.data.decode("utf-8"))
        event["ingested_at"] = datetime.now(timezone.utc).isoformat()
        batch.append(event)

        print(f"Received: {event['event_type']} for order {event['order_id'][:8]}...")

        if len(batch) >= BATCH_SIZE:
            flush_batch()

        message.ack()
    except Exception as e:
        print(f"  Error processing message: {e}")
        message.nack()


def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    print(f"Listening on {subscription_path}")
    print(f"Streaming into {table_ref}")
    print("Press Ctrl+C to stop\n")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        flush_batch()
        print("\nSubscriber stopped, final batch flushed.")


if __name__ == "__main__":
    main()
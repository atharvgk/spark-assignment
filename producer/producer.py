import json
import random
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import os

# -----------------------
# Configuration
# -----------------------
TOPIC = os.getenv("KAFKA_TOPIC", "order_events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EVENT_INTERVAL_SEC = float(os.getenv("EVENT_INTERVAL_SEC", "0.5"))

# -----------------------
# Logging Configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

# -----------------------
# Kafka Producer
# -----------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",              # Ensure leader + replicas acknowledge
    retries=3,
    linger_ms=10
)

EVENT_TYPES = ["CREATED", "UPDATED", "CANCELLED"]

def generate_event():
    """
    Generates a single lightweight event.
    Event time is skewed to allow out-of-order arrivals.
    """
    base_time = datetime.utcnow()
    event_time = base_time + timedelta(seconds=random.randint(-120, 30))

    return {
        "order_id": f"order_{random.randint(1, 50)}",
        "customer_id": f"cust_{random.randint(1, 20)}",
        "product_id": f"prod_{random.randint(1, 100)}",
        "event_type": random.choices(
            EVENT_TYPES, weights=[0.6, 0.25, 0.15]
        )[0],
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 200), 2),
        "event_time": event_time.isoformat()
    }

def send_event(event):
    """
    Sends an event to Kafka and waits for broker acknowledgement.
    This confirms the message was written successfully.
    """
    future = producer.send(TOPIC, event)

    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Produced event | topic=%s partition=%s offset=%s order_id=%s event_type=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
            event["order_id"],
            event["event_type"]
        )
    except Exception as e:
        logger.error("Failed to produce event: %s", e)

def run():
    logger.info("Starting Kafka producer (continuous mode)...")

    while True:
        event = generate_event()
        send_event(event)

        # Introduce duplicates (~10%)
        if random.random() < 0.10:
            send_event(event)

        time.sleep(EVENT_INTERVAL_SEC)

if __name__ == "__main__":
    run()

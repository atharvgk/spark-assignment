import json
import logging
from kafka import KafkaConsumer
import os

# -----------------------
# Configuration
# -----------------------
TOPIC = os.getenv("KAFKA_TOPIC", "order_events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-consumer-group")

# -----------------------
# Logging Configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------
# Kafka Consumer
# -----------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    max_poll_records=100
)

def run():
    logger.info(f"Starting Kafka consumer for topic '{TOPIC}' with group '{GROUP_ID}'...")
    
    try:
        for message in consumer:
            event = message.value
            logger.info(
                "Consumed event | topic=%s partition=%s offset=%s order_id=%s event_type=%s",
                message.topic,
                message.partition,
                message.offset,
                event.get("order_id"),
                event.get("event_type")
            )
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    run()

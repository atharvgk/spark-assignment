import os
import socket
import time
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def parse_bootstrap(bootstrap):
    first = bootstrap.split(",")[0].strip()
    if ":" in first:
        host, port = first.split(":", 1)
        try:
            port = int(port)
        except Exception:
            port = 9092
    else:
        host = first
        port = 9092
    return host, port


def wait_for(host, port, timeout=120, interval=1):
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=3):
                logger.info("Connection to %s:%d succeeded", host, port)
                return True
        except Exception:
            elapsed = time.time() - start
            if timeout and elapsed > timeout:
                logger.warning("Timeout waiting for %s:%d after %ds", host, port, int(elapsed))
                return False
            logger.info("Waiting for %s:%d ...", host, port)
            time.sleep(interval)


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    host, port = parse_bootstrap(bootstrap)
    wait_for(host, port)
    os.execvp("python", ["python", "producer.py"])


if __name__ == "__main__":
    main()

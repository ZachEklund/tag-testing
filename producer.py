"""Stream live Wikipedia edits from Wikimedia EventStreams into a Kafka topic."""

import json
import os
import signal
import sys

import requests
from confluent_kafka import Producer

# Settings come from environment variables so they can be changed at
# `docker run -e ...` time without rebuilding the image.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wikipedia-changes")
STREAM_URL = os.getenv(
    "WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)

# Wikimedia rejects requests without a descriptive User-Agent.
HEADERS = {
    "User-Agent": "wikipedia-producer-class-demo/1.0 (python-requests)",
    "Accept": "text/event-stream",
}

running = True


def handle_shutdown(signum, frame):
    """Stop the main loop on Ctrl+C (SIGINT) or `docker stop` (SIGTERM)."""
    global running
    running = False


def delivery_report(err, msg):
    """Called once per message by Kafka to report success or failure."""
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


def stream_events(url):
    """Yield parsed JSON events from a Server-Sent Events (SSE) stream.

    SSE sends text lines; the ones we want look like `data: {...json...}`.
    """
    with requests.get(url, headers=HEADERS, stream=True, timeout=(10, 60)) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            if not running:
                break
            if not line or not line.startswith("data:"):
                continue  # skip blank keep-alives and `event:`/`id:` lines
            try:
                yield json.loads(line[len("data:"):].strip())
            except json.JSONDecodeError:
                continue  # skip any malformed payload


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(f"Producing to '{TOPIC}' on {BOOTSTRAP_SERVERS} from {STREAM_URL}")

    count = 0
    try:
        for event in stream_events(STREAM_URL):
            # Key by wiki (e.g. "enwiki") so edits from the same wiki land
            # in the same partition and stay in order.
            producer.produce(
                TOPIC,
                key=event.get("wiki", ""),
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)  # serve delivery callbacks without blocking
            count += 1
            if count % 100 == 0:
                print(f"Sent {count} events")
    finally:
        print("Flushing remaining messages...")
        producer.flush(10)  # wait up to 10s for queued messages to send
        print(f"Done. Sent {count} events total.")


if __name__ == "__main__":
    main()

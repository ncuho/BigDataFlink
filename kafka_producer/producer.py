import csv
import json
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

DATA_DIR = "/data"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "petstore_sales"


def wait_for_kafka(bootstrap_servers, max_retries=30, delay=2):
    """Wait until Kafka is reachable."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            print(f"Kafka is ready (attempt {attempt})")
            return
        except NoBrokersAvailable:
            print(f"Kafka not available yet (attempt {attempt}/{max_retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Kafka did not become available in time")


def get_csv_files(data_dir):
    """Return sorted list of MOCK_DATA CSV files."""
    files = sorted(
        f for f in os.listdir(data_dir)
        if f.startswith("MOCK_DATA") and f.endswith(".csv")
    )
    print(f"Found {len(files)} CSV files: {files}")
    return files


def main():
    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
    )

    csv_files = get_csv_files(DATA_DIR)
    total_messages = 0

    for filename in csv_files:
        filepath = os.path.join(DATA_DIR, filename)
        print(f"Processing file: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                message = dict(row)
                producer.send(TOPIC_NAME, value=message)
                total_messages += 1

                # небольшой sleep для эмуляции потокового источника
                if total_messages % 100 == 0:
                    print(f"Sent {total_messages} messages so far...")
                    time.sleep(0.1)

        print(f"Finished file: {filename}")

    producer.flush()
    producer.close()
    print(f"All done. Total messages sent: {total_messages}")


if __name__ == "__main__":
    main()
import json
from datetime import datetime, timezone
from pynput import keyboard
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "topic_space_event"

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka message delivered to "
            f"{msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}"
        )


def on_press(key):
    if key == keyboard.Key.space:
        now = datetime.now(timezone.utc)
        timestamp = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

        timestamp_epoch = int(now.timestamp() * 1000)

        data = {
            "timestamp": timestamp,
            "timestamp_epoch": timestamp_epoch,
            "key": "space"
        }

        print(f"json results: {data}")

        # Convert Python dictionary to JSON
        message = json.dumps(data)

        # Send message to Kafka
        producer.produce(
            topic=KAFKA_TOPIC,
            value=message.encode("utf-8"),
            callback=delivery_report
        )

        # Process Kafka events
        producer.poll(0)


def on_release(key):
    if key == keyboard.Key.esc:
        return False


try:
    with keyboard.Listener(
        on_press=on_press,
        on_release=on_release
    ) as listener:
        listener.join()

finally:
    # Wait for pending messages to be delivered
    producer.flush()

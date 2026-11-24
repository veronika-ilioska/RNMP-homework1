"""Read a finite, read-only Kafka snapshot for the required offset comparison."""

import time

from kafka import KafkaConsumer, TopicPartition


def read_snapshot(topic, bootstrap_servers, timeout):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers.split(","), group_id=None,
                             enable_auto_commit=False, receive_message_max_bytes=64 * 1024 * 1024)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise ValueError(f"No partitions found for {topic}")
        assigned = [TopicPartition(topic, partition) for partition in sorted(partitions)]
        consumer.assign(assigned)
        ends = consumer.end_offsets(assigned)
        consumer.seek_to_beginning(*assigned)
        messages = []
        deadline = time.monotonic() + timeout
        while any(consumer.position(partition) < ends[partition] for partition in assigned):
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out reading the Kafka topic snapshot")
            for partition, batch in consumer.poll(timeout_ms=1000).items():
                messages.extend(message for message in batch if message.offset < ends[partition])
        return messages
    finally:
        consumer.close()

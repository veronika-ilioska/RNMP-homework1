"""Replay GitHub archive JSON Lines events into Kafka with simulated lateness."""

import argparse
from dataclasses import dataclass
from datetime import datetime
import gzip
import json
import logging
import math
from pathlib import Path
import random
import sys
import time


@dataclass
class Event:
    timestamp_ms: int
    key: str
    value: dict


@dataclass
class ScheduledEvent:
    due_ms: int
    delay_ms: int
    event: Event


def field(record, path):
    """Resolve nested fields such as actor.id or actor.login."""
    value = record
    for part in path.split("."):
        value = value[part]
    return value


def load_events(path, timestamp_field="created_at", key_field="actor.id"):
    """Read JSONL (optionally gzipped), validate it, then sort by event time."""
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    events = []
    with opener(path, "rt", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                raw_timestamp = field(record, timestamp_field)
                timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    raise ValueError("event timestamps must include a timezone (e.g. Z)")
                timestamp_ms = int(timestamp.timestamp() * 1000)
                if timestamp_ms < 0:
                    raise ValueError("event timestamps must be on or after 1970-01-01")
                key = field(record, key_field)
                if isinstance(key, bool) or not isinstance(key, (str, int)) or str(key) == "":
                    raise ValueError("Kafka key must be a nonempty string or integer")
                # Keep the original GitHub event, adding the class example's fields.
                value = dict(record)
                value["key"] = str(key)
                value["timestamp"] = timestamp_ms
                events.append(Event(timestamp_ms, str(key), value))
            except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
                raise ValueError(f"{path}, line {line_number}: {error}") from error
    if not events:
        raise ValueError(f"No events found in {path}")
    events.sort(key=lambda event: event.timestamp_ms)
    return events


def schedule_events(events, late_fraction, delay_seconds):
    """Hold selected events back while newer events continue to be released.

    Delay is measured in DATA seconds, and is divided by playback speed later.
    Sorting by release time also drains delayed events after the input ends.
    """
    delayed_indices = set(random.sample(range(len(events)), round(len(events) * late_fraction)))
    delay_ms = round(delay_seconds * 1000)
    schedule = []
    for index, event in enumerate(events):
        event_delay = delay_ms if index in delayed_indices else 0
        schedule.append(ScheduledEvent(event.timestamp_ms + event_delay, event_delay, event))
    schedule.sort(key=lambda scheduled: scheduled.due_ms)
    return schedule


def replay(schedule, origin_ms, speed, emit):
    """Use absolute deadlines to avoid accumulating per-message sleep drift."""
    started = time.monotonic()
    stats = {"sent": 0, "delayed": 0, "out_of_order": 0, "out_of_order_same_key": 0}
    max_timestamp = -1
    max_by_key = {}
    for scheduled in schedule:
        target = (scheduled.due_ms - origin_ms) / 1000 / speed
        remaining = started + target - time.monotonic()
        while remaining > 0:
            time.sleep(remaining)
            remaining = started + target - time.monotonic()
        emit(scheduled)
        event = scheduled.event
        stats["sent"] += 1
        stats["delayed"] += scheduled.delay_ms > 0
        stats["out_of_order"] += event.timestamp_ms < max_timestamp
        stats["out_of_order_same_key"] += event.timestamp_ms < max_by_key.get(event.key, -1)
        max_timestamp = max(max_timestamp, event.timestamp_ms)
        max_by_key[event.key] = max(max_by_key.get(event.key, -1), event.timestamp_ms)
    return stats


def create_producer(bootstrap_servers):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        security_protocol="PLAINTEXT",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
        max_block_ms=10000,
    )


def prepare_topic(bootstrap_servers, topic):
    """Keep historical event timestamps on newly created homework topics."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers.split(","))
    try:
        admin.create_topics([NewTopic(topic, 1, 1, topic_configs={"retention.ms": "-1"})])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def send_event(producer, topic, scheduled):
    event = scheduled.event
    # Set BOTH the JSON event-time field and Kafka's record timestamp.
    # Waiting for acknowledgement also surfaces delivery errors immediately.
    producer.send(topic, key=event.key, value=event.value,
                  timestamp_ms=event.timestamp_ms).get(timeout=30)


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="JSONL or .json.gz input file")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="github-events")
    parser.add_argument("--timestamp-field", default="created_at", help="ISO 8601 field with timezone")
    parser.add_argument("--key-field", default="actor.id", help="Nested field used as the Kafka key (default: acting user ID)")
    parser.add_argument("--speed", type=float, default=60, help="Data seconds per real second (60 = 1h in 1min)")
    parser.add_argument("--late-fraction", type=float, default=0.1, help="Fraction of records to delay, from 0 to 1")
    parser.add_argument("--delay-seconds", type=float, default=120, help="Extra delay in DATA seconds")
    args = parser.parse_args()
    if not math.isfinite(args.speed) or args.speed <= 0:
        parser.error("--speed must be finite and greater than zero")
    if not math.isfinite(args.late_fraction) or not 0 <= args.late_fraction <= 1:
        parser.error("--late-fraction must be between 0 and 1")
    if not math.isfinite(args.delay_seconds) or args.delay_seconds < 0:
        parser.error("--delay-seconds must be finite and nonnegative")
    return args


def main():
    args = arguments()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    producer = None
    try:
        events = load_events(args.input, args.timestamp_field, args.key_field)
        schedule = schedule_events(events, args.late_fraction, args.delay_seconds)
        logging.info("Loaded %s events; speed=%sx; planned duration=%.2fs",
                     len(events), args.speed,
                     (schedule[-1].due_ms - events[0].timestamp_ms) / 1000 / args.speed)
        prepare_topic(args.bootstrap_servers, args.topic)
        producer = create_producer(args.bootstrap_servers)
        stats = replay(schedule, events[0].timestamp_ms, args.speed,
                       lambda scheduled: send_event(producer, args.topic, scheduled))
        producer.flush(timeout=30)
        logging.info("Delivery complete: %s", stats)
        return 0
    except KeyboardInterrupt:
        logging.warning("Stopped by user; input replay is incomplete")
        return 130
    except Exception as error:
        logging.error("Producer failed: %s", error)
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=10)


if __name__ == "__main__":
    sys.exit(main())

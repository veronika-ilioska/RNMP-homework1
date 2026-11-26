# Course Python base for PyCharm

Open this `python` folder in PyCharm. Use `.venv/Scripts/python.exe` as the
project interpreter. The producer, consumer, dashboard, and `requirements.txt`
are all in this folder. Run the commands below from this folder.

This folder is based on [RNMP_homework1](https://github.com/stefanandonov/RNMP_homework1).
Its original consumer, Kafka Compose file and dependencies are retained.
`produce_messages.py` replaces the random sensor producer with the required
GitHub dataset producer.

## How does it work?

The producer reads your `2026-10-15-15.json.gz` file directly; no extraction is
needed. It contains one GitHub event per line. Defaults:

- Event time: `created_at`, preserved in the JSON and converted to epoch
  milliseconds in the added `timestamp` field and Kafka record timestamp.
- Kafka key: `actor.id`, encoded as a UTF-8 string. This identifies the user
  who performed the GitHub event. The Java jobs group by this acting user across
  repositories. `--key-field` configures the producer key, and the Java jobs
  derive their user identity from `actor.id` in each JSON event.
- Topic: `github-events`; broker: `localhost:9092`.
- Speed: `60` data seconds per real second, so one hour takes one minute.
- Delayed records: 10%, selected randomly on each run, each held back by
  120 data seconds (2 real seconds at speed 60).

The original event fields, including the nested GitHub payload, are retained.
Top-level `key` and `timestamp` are added using the class example's naming.
If the input already contains those two fields, their values are replaced.

## Run in PowerShell

From this project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d --wait
.\.venv\Scripts\python.exe produce_messages.py --input "$env:USERPROFILE\Downloads\2026-10-15-15.json.gz" --speed 60 --late-fraction 0.1 --delay-seconds 120
```

The producer exits after all records, including delayed records, are sent.
The replay may finish slightly beyond one minute because delayed records at
the end must also be sent. Kafka acknowledgements can also affect duration.
Kafka's automatic topic creation is enabled in the class configuration.

To inspect messages, run in a second terminal:

```powershell
.\.venv\Scripts\python.exe consume_messages.py github-user-sessions-multi1
```

This is the original course consumer. From Git Bash in this folder:

```bash
./run.sh consume_messages.py github-user-sessions-multi1
# In another terminal:
./run.sh consume_messages.py github-user-alerts-multi1
```

It prints `Received message: <JSON>` and takes the topic as the first argument.
It uses `localhost:9092` and the course group `my_consumer_group`, with earliest
reset and auto-commit disabled. Multiple consumers of the same topic share its
partitions. Without a topic argument it keeps the class default, `sensors`.
The dashboard supplies keys, offsets and record timestamps separately.
Use fresh topic names for each clean run; Kafka topics are append-only.

## How the timing works

1. Read and sort the input by event time. Your source file already contains
   some out-of-order records, so this step establishes an ordered baseline.
2. Select `round(record_count * late_fraction)` records and add the configured
   delay to their scheduled release times, keeping their event times intact.
3. Sort by release time. Newer events can now pass delayed older events.
4. Send each record at its absolute monotonic-clock deadline:
   `elapsed_real_seconds = (event_time - first_event_time + delay) / speed`.
5. Send with `key=actor.id`, the JSON value, and `timestamp_ms=event_time`.
   Wait for acknowledgement and flush/close the producer at completion.

For example, with events at 15:00:00, 15:00:10, and 15:00:20, delaying the
first by 30 data seconds produces the event-time order 15:00:10, 15:00:20,
15:00:00. Simply sleeping before sending an older event would block newer
events too; scheduling by release time allows them to overtake it.

The summary counts delayed records, globally out-of-order records, and
out-of-order records for the same key. The exact amount of disorder depends
on event spacing and the selected records. Delaying every record equally
(`--late-fraction 1`) shifts the entire timeline and preserves order.

Watermarks belong in the downstream processor (such as Flink). Extract
`timestamp` as event time there. A watermark allowance below the configured
delay can expose late events; an allowance above it can accommodate them.
Delay and watermark allowances use **data time**, not accelerated wall time.
Topics default to three partitions (`--partitions 3`). Kafka routes user keys
across partitions 0, 1 and 2; ordering is per partition, not global. Events for
the same user keep the same partition while the partition count is unchanged.


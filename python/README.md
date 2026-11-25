# Course Python base for PyCharm

Open this `python` folder in PyCharm. Use `.venv/Scripts/python.exe` as the
project interpreter. The producer, consumer, dashboard, and `requirements.txt`
are all in this folder. Run the commands below from this folder.

This folder is based on [RNMP_homework1](https://github.com/stefanandonov/RNMP_homework1).
Its original consumer, Kafka Compose file and dependencies are retained.
`produce_messages.py` replaces the random sensor producer with the required
GitHub dataset producer. The shell runner/startup are adapted for this dataset
and the Windows/Linux `.venv`; the dashboard and comparison are additions.
No additional clone is needed. The Java project remains separate in `../java`.
See [the workspace guide](../README.md) for the complete flow.

## Run with PyCharm's green Run button

Open this `python` folder and set the project interpreter to
`.venv\Scripts\python.exe`.
The `.run` directory supplies these saved configurations in the Run selector:

- **Producer - IDE live** sends your dataset to `github-users-multi1` at speed 60.
- **Consumer - Sessions** displays JSON from `github-user-sessions-multi1`.
- **Consumer - Notifications** displays JSON from `github-user-alerts-multi1`.
- **Dashboard - Stream observatory** starts the web dashboard at
  `http://127.0.0.1:8501`, monitoring the same three IDE topics.

Start Docker Desktop, then start Kafka from PyCharm's Terminal in this folder:

```powershell
docker compose up -d --wait
```

Start IntelliJ's **GitHub - Live (both modules)** first, then the dashboard
and two consumers, and finally **Producer - IDE live**. For the data already in `github-users-multi1`, run the
consumers and IntelliJ's **GitHub - Replay (both modules)** instead.
The live job stays running after the producer finishes and needs advancing event
time to close the final sessions. Replay closes them automatically at its end.
Edit **Run > Edit Configurations > Parameters** to change topics, dataset, or
speed; keep the topics consistent between producer, Java, and consumers.

The dashboard can replace the two terminal consumers for visual inspection:

```powershell
.\.venv\Scripts\python.exe dashboard.py
```

It uses the existing `kafka-python` dependency and Python's standard HTTP server;
no Node installation or frontend build is needed. It connects independently with
no committed group offsets. Use **Connection settings** to inspect other topics.
Its own earliest/latest selection does not change the Java application's offset
selection. Full usage and the distinction between preview and Flink-confirmed
sessions are in [the workspace guide](../README.md).

Uses the course's `kafka-python==2.3.3` dependency and its unmodified Kafka Docker
configuration. The root Compose file runs only Flink.
The producer creates a new input topic with `retention.ms=-1` to retain historical
event timestamps. Existing topics keep their configuration.
Stop course Kafka with `docker compose down` when finished.

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

For an ordered baseline, use `--late-fraction 0`.
Use `python produce_messages.py --help` for the available options.
Inputs may be `.jsonl`, line-delimited `.json`, or their gzip-compressed versions.
Timestamps must be ISO 8601 strings with a timezone, such as `2026-10-15T15:00:00Z`.

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

This homework implementation loads and sorts the input in memory, which is
suitable for this hourly file. Delivery waits for acknowledgements and uses
retries; it does not provide end-to-end exactly-once replay or restart checkpoints.


## Compare earliest and latest

With course Kafka and this project's updated Docker Flink services running, run this from
the Python folder:

```powershell
.\.venv\Scripts\python.exe compare_offsets.py --input "$env:USERPROFILE\Downloads\2026-10-15-15.json.gz"
```

The script produces the same data twice into fresh Kafka topics, starts the
Java jobs at the requested offsets, compares both JSON outputs, and records
timing and watermark behavior in `../reports/`. It publishes one additional
JSON message with the comparison to its own Kafka topic. The existing result
topics are not reused. See [the workspace instructions](../README.md) for
building and starting the Java services.


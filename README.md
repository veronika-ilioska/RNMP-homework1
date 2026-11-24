# GitHub events homework

The workspace contains two IDE projects built on the course's Kafka setup:

```text
hw1-231035/
|-- python/                Course Python base; open in PyCharm
|   |-- docker-compose.yml  Course Kafka setup
|   |-- run.sh / start.sh / stop.sh
|   |-- produce_messages.py Custom dataset producer
|   |-- consume_messages.py Original course consumer
|   |-- dashboard.py       Local Kafka dashboard server
|   |-- dashboard/         HTML, CSS and JavaScript
|   |-- compare_offsets.py
|   |-- requirements.txt
|   |-- .venv/
|   `-- README.md
|-- java/                  Open in IntelliJ IDEA as a Maven project
|   |-- pom.xml
|   |-- src/main/java/     Analytics and event-driven Java jobs
|   |-- Dockerfile
|   |-- run_jobs.ps1
|   `-- README.md
`-- docker-compose.yml     Flink only; joins the course Kafka network
```

- **PyCharm:** open the `python` folder and select `python/.venv/Scripts/python.exe`
  as its interpreter. Setup and producer instructions: [Python README](python/README.md).
- **IntelliJ IDEA:** open the `java` folder / `java/pom.xml` as a Maven project,
  with Java 17. Job behavior and build instructions: [Java README](java/README.md).

## Dataset and first-time setup

Dataset: **`2026-10-15-15.json.gz`**, supplied during class. Obtain that file from
the course materials and save it in your Downloads directory. The
dataset is identified here by filename; no public download URL is assumed.
The file is not included in the repository or submission ZIP. The recorded
experiment used 3,582 events from 2,474 acting users, grouped by `actor.id`.

Install Docker Desktop, Git, Python 3.10+ and, for a native IntelliJ run, Java 17.
The Maven application and Docker runtime use **Flink 2.3.0 and Java 17**.
Run these commands in PowerShell from this workspace root:

```powershell
python -m venv python/.venv
.\python\.venv\Scripts\python.exe -m pip install -r python/requirements.txt
docker compose -f python/docker-compose.yml up -d --wait
```

The Maven Flink application is `java/pom.xml`; its entry point is
`mk.ukim.rnmp.GitHubPipeline`. It contains both the analytical and event-driven
modules. The custom dashboard is separate from Flink's cluster administration UI.

The Java code follows the supplied `flink-demo (1).zip` exercises: `Common`
helpers, event-time watermarks (Step 3), incremental aggregation (Step 5),
keyed state/timers (Step 6) and session windows (Step 7). See the
[exercise-to-project mapping](java/README.md#following-the-class-exercises).

### The course repository is the Python base

`python/` contains the base files from
[RNMP_homework1](https://github.com/stefanandonov/RNMP_homework1)
(commit `63175727accc4d3c5b963358376a07ea7fee9067`). The original
`consume_messages.py`, Kafka `docker-compose.yml`, and requirements are retained.
`produce_messages.py` is the homework implementation replacing the sample sensor
producer: it reads your dataset, sends user-keyed JSON in event time, simulates
speed and delayed arrivals. The dashboard and comparison scripts are additions.

The base runner uses the course's command convention, adapted for the PyCharm
`.venv` on Windows/Linux. `start.sh` starts this Kafka and then our producer;
`stop.sh` is the course shutdown helper. Everything needed is included in the
repository/ZIP, so no separate course clone is needed to run it.

From Git Bash in `python/`, use the same workflow as in class:

```bash
docker compose up -d --wait
./run.sh consume_messages.py github-user-sessions-repo1
```

Use a second terminal for `./run.sh consume_messages.py github-user-alerts-repo1`.
The original consumer prints `Received message: <JSON>` and accepts the topic as
its first argument. It connects to `localhost:9092`, uses
`my_consumer_group`, starts from earliest when no offset is committed, and disables
auto-commit. Always pass a GitHub topic; its original fallback is `sensors`.
Multiple copies of that consumer on the same topic share its partitions.
The dashboard separately shows Kafka keys, timestamps and offsets.

From this workspace root, `./run.sh` and `./start.sh` forward to the Python base.
The root Compose file runs only Flink and joins the Python Kafka network,
`python_default`. IDE Python/Java use `localhost:9092`; Docker Flink uses
`kafka:9093`. The Java launcher starts `python/docker-compose.yml` and discovers
its network automatically. With a custom Compose project name, set
`KAFKA_NETWORK` before direct root Compose commands.

New producer/input and Java output topics use `retention.ms=-1` so historical
event timestamps remain available for replay. Existing topics keep their settings.
The course Kafka configuration itself stays unchanged.

### Switching from the previous project Kafka

If the previous homework stack is still running, finish its jobs, then stop its
containers before starting the course broker:

```powershell
docker stop hw1-231035-taskmanager-1 hw1-231035-jobmanager-1 hw1-231035-kafka-1
docker compose -f python/docker-compose.yml up -d --wait
```

`stop` keeps the old containers. The new course broker has its own data, so run
the producer again for the input topic you select. The IDE producer, consumers,
Java job and dashboard all connect to the course broker on `localhost:9092`.

### Kafka connector version

The project pins `flink-connector-kafka:5.0.0-2.2` and Flink 2.3.0, matching the
supplied exercise POM. Apache's
[Flink 2.3 Kafka documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kafka/)
currently lists no connector release specifically for 2.3. This combination is
verified for this application's Kafka source, JSON sinks, windows, state and
timers in the recorded earliest/latest run; it is not an upstream compatibility
guarantee for every connector feature.

## Dashboard and IDE Run buttons

In PyCharm, open `python`, select `python/.venv/Scripts/python.exe`, and run
**Dashboard - Stream observatory**. Open **http://127.0.0.1:8501**.
In IntelliJ, open `java`, reload Maven and select Java 17.

For a live demonstration, start the dashboard, then IntelliJ's
**GitHub - Live (both modules)**, then PyCharm's **Producer - IDE live**.
These configurations consistently use:

| Purpose | Kafka topic |
|---|---|
| Producer input / Flink source | `github-users-repo1` |
| Completed 30-minute-gap sessions | `github-user-sessions-repo1` |
| Per-repository inactivity alerts | `github-user-alerts-repo1` |

The same live run can be started from three PowerShell terminals at the root:

```powershell
# Terminal 1: dashboard
.\python\.venv\Scripts\python.exe python/dashboard.py

# Terminal 2: build/start Flink and submit both Java branches
powershell -ExecutionPolicy Bypass -File java/run_jobs.ps1 -Build -Mode stream -StartOffset earliest -InputTopic github-users-repo1 -AnalyticsTopic github-user-sessions-repo1 -EventsTopic github-user-alerts-repo1

# Terminal 3: producer, once Java is running
.\python\.venv\Scripts\python.exe python/produce_messages.py --input "$env:USERPROFILE\Downloads\2026-10-15-15.json.gz" --topic github-users-repo1 --speed 60
```

The dashboard shows Kafka input records, independently consumed records,
offset/lag, input arrival rate, out-of-order arrivals, event types, user session
timelines, completed sessions and notifications. Use the user filter, click a row
to inspect its Kafka envelope, pause the visible refresh, or export the current
view as JSON. **Connection settings** switches the three monitored topics and
the dashboard's own earliest/latest starting offset. It does not change Flink's
offsets or launch producers/jobs.

Solid intervals come from real Flink session results. Striped intervals are
previews from the retained input, using a 30-minute gap. The preview is not Flink
state or a confirmed result. Rates measure dashboard-observed arrivals, including
historical catch-up. Flink consumption counters/watermarks are shown only if a
matching job is available through the configured REST endpoint. A native IntelliJ
run does not need REST for the Kafka tables and session visualization to work.
Input payload bodies are omitted from the dashboard summaries to bound memory;
the producer still sends the original JSON payload. The view retains up to 20,000
input summaries, 10,000 session results, and 2,000 notifications. Table views show
the latest 150 input records/notifications and 1,000 session results.

For data already in `github-users-run3`, choose IntelliJ's
**GitHub - Replay run3 (both modules)** and set the dashboard input topic to
`github-users-run3` (the output topics remain the IDE topics above).
Use fresh input and output topic names for each clean run. After a live producer
finishes, final sessions can remain open until event time advances or the Docker
job is explicitly drained. A complete bounded replay flushes them automatically.

## Submission files

The short Macedonian documentation is [the project PDF](output/pdf/project-documentation.pdf).
It covers the dataset and key, architecture, both Java modules, watermarks,
dashboard, and measured earliest/latest comparison. Its reproducible source is
`docs/build_documentation.py`; install `docs/requirements.txt` to regenerate it.

Build the data-free submission archive from the workspace root:

```powershell
.\python\.venv\Scripts\python.exe python/package_submission.py
```

The result is `output/submission/hw1-231035.zip`. Its allowlist includes source,
Maven configuration, IDE Run configurations, dashboard assets, README files,
PDF documentation and aggregate comparison reports. It excludes datasets,
virtual environments, `.idea`, compiled targets, local databases and runtime logs.
The course Python base files are included; Git metadata is excluded.
Upload this ZIP to Moodle, or share your own Git repository with the assistant.

## Producer shortcut

With Docker Desktop open, run this from Git Bash at the workspace root:

```bash
bash start.sh --topic github-users-repo1 --speed 60
```

The script delegates to `python/start.sh`, starts Kafka from the Python base,
waits for its health check, then starts the producer using
its Python environment and the dataset in Downloads. Pass `--input` for another
file. For live results, start the dashboard and Java job before the producer.

The analytics use event-time sessions with a 30-minute inactivity gap per user.
Each event extends the session to 30 data minutes after the latest event. At
60x speed, the gap plus the default 3-minute watermark allowance takes about
33 real seconds after that user's last event, provided input time keeps advancing.
A stopped live input can leave the final sessions pending; replay or a final
Docker Flink drain closes them.

## Run a complete replay

The following commands run from the workspace root. Choose unused topic names
for each replay, because Kafka topics are append-only.

```powershell
docker compose -f python/docker-compose.yml up -d --wait
.\python\.venv\Scripts\python.exe .\python\produce_messages.py --input "C:\Users\ilios\Downloads\2026-10-15-15.json.gz" --topic github-events-users-split --speed 60
.\java\run_jobs.ps1 -InputTopic github-events-users-split -AnalyticsTopic github-user-activity-split -EventsTopic github-user-alerts-split
```

From the Java project's terminal, use `.\run_jobs.ps1`; from the Python project's
terminal, use `.\.venv\Scripts\python.exe produce_messages.py ...`. The Java runner
uses the Python base Compose file for Kafka and the parent Compose file for Flink,
including when launched from another working directory. From `python`, the
Kafka file is `docker-compose.yml` in the Python base.

The producer writes JSON events to Kafka. Java analytics use user session
windows across repositories with a 30-minute gap; the event-driven job uses keyed state and event-time timers
to notify when a user has no activity in a particular repository for 30 event-time minutes. Both outputs retain
the acting user Kafka key (`actor.id`). Defaults use a 180-second watermark allowance.

The measured session and repository-alert counts, timings, and earliest/latest
comparison are recorded in `reports/offset-comparison.json`.

Flink dashboard: http://localhost:8081. Stop Flink from this root with
`docker compose --profile processing down`. Stop course Kafka separately with
`docker compose -f python/docker-compose.yml down`.

## Earliest/latest comparison

Java accepts `--start-offset earliest|latest` (`-StartOffset` in the PowerShell
launcher). This chooses the first offset for a fresh job. `-Mode replay` stops
at a snapshot of the input's end offsets; `-Mode stream` keeps polling Kafka.

Reproduce the full comparison from this root:

```powershell
docker compose -f python/docker-compose.yml up -d --wait
docker compose --profile processing build jobmanager
docker compose --profile processing up -d --wait jobmanager taskmanager
.\python\.venv\Scripts\python.exe .\python\compare_offsets.py --input "C:\Users\ilios\Downloads\2026-10-15-15.json.gz"
```

The comparison uses fresh Kafka topics, records the latest source position before
starting its producer, measures both jobs, and compares their JSON results before
and after an explicit final drain.
The checked-in report was measured with the course broker on isolated host port
19092 to keep the previous running stack available. Normal startup uses 9092;
Docker Flink still connects to `kafka:9093` on the course network.

See the [comparison report](reports/offset-comparison.md) and
[raw measurements](reports/offset-comparison.json). The experiment publishes
the measured comparison to Kafka as JSON as well.

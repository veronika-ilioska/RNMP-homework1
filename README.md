# GitHub activity streaming project

Start Kafka, run the Java Flink application, then start the Python producer.
The dashboard shows the input events, completed sessions and inactivity alerts.

- **python/**: course Kafka setup and original consumer, custom producer and dashboard. Open in **PyCharm**.
- **java/**: Flink 2.3.0 Maven application. Open in **IntelliJ IDEA**, using **Java 17**.

## 1. First-time setup

Install Docker Desktop, Python 3.10+, Java 17, PyCharm and IntelliJ IDEA.
Git is needed if you clone the repository.

Get **2026-10-15-15.json.gz** from the course materials and save it in Downloads.
The dataset is not included in this repository. No extraction is needed.

Open PowerShell in the project root: the folder containing this README.
Create the Python environment and install dependencies once:

```powershell
python -m venv python/.venv
.\python\.venv\Scripts\python.exe -m pip install -r python/requirements.txt
```

**Configure PyCharm:**

1. Use **File > Open** to open the project's `python` folder.
2. Open Settings with **Ctrl + Alt + S** and search for **Python Interpreter**.
3. Select the existing interpreter `python\.venv\Scripts\python.exe` from this project.
4. The saved Run configurations should appear in the Run selector.

**Configure IntelliJ:**

1. Use **File > Open** to open the project's `java` folder and load `pom.xml` as Maven.
2. Set the project SDK and Maven runner to **Java 17**, then reload Maven.
3. The saved Run configuration **GitHub - Live (both modules)** should appear.

## 2. Start Kafka

Open Docker Desktop and wait until its engine is running.
Run these commands in PowerShell **from the project root**:

```powershell
docker compose -f python/docker-compose.yml up -d --wait
docker compose -f python/docker-compose.yml ps
```

Continue when the `kafka` container is **healthy**. Kafka is available at `localhost:9092`.

If your terminal is already inside `python`, the equivalent command is:

```powershell
docker compose up -d --wait
```

## 3. Start the Java application in IntelliJ

Select **GitHub - Live (both modules)** and click **Run**.
Wait for it to start without errors. Leave it running.

This one application runs both modules:

- **Analytics:** groups events by `actor.id` and creates event-time sessions with a 30-minute inactivity gap.
- **Event-driven:** emits an alert when a user has no activity in a repository for 30 event-time minutes.

The saved configuration uses streaming mode, earliest offsets and `localhost:9092`.
Flink starts locally inside IntelliJ. **You do not need to start Docker JobManager or TaskManager containers for this workflow.**

## 4. Start the dashboard and consumers in PyCharm

Run **Dashboard - Stream observatory**, then open:

**http://127.0.0.1:8501**

To also see the JSON results in the console, run these saved configurations:

- **Consumer - Sessions**
- **Consumer - Notifications**

These use the original course consumer. From Git Bash inside `python`, you can
run the same consumers in two separate terminals:

```bash
./run.sh consume_messages.py github-user-sessions-multi1
```

```bash
./run.sh consume_messages.py github-user-alerts-multi1
```

## 5. Start the producer last

In PyCharm, select **Producer - IDE live** and click **Run**.

Set `--input` in **Run > Edit Configurations > Parameters** to the location of
`2026-10-15-15.json.gz` on your computer.

The producer sends the file at 60x speed: one hour of event time targets about
one real minute, plus delivery overhead. It randomly selects and delays 10% of records by 120 data
seconds. Flink allows 180 data seconds of out-of-order arrival.

**Startup order: Kafka → Java → dashboard/consumers → producer.**

## 6. What you should see

| Dashboard item | Meaning |
|---|---|
| Kafka connected | The dashboard can communicate with Kafka. |
| Input records | Events stored in the input topic. |
| Dashboard consumed | Events observed by the dashboard, not a Flink processing counter. |
| Completed sessions | Results read from Flink's session output topic. |
| Repository inactivity alerts | Results read from Flink's notification topic. |

Input events appear first. Sessions and alerts appear as event time and
watermarks advance. A 30-minute gap is measured in **dataset time**, not 30 minutes
of real waiting. Sessions can last longer than 30 minutes while activity continues.

When the producer finishes, the live Java application keeps running. Final
windows and timers can remain pending because Kafka has not signalled the end
of the stream. A bounded replay or `stop --drain` closes the remaining results;
see the [Java guide](java/README.md).

In the timeline, striped intervals are input previews; solid intervals are
confirmed Flink results. The optional Flink REST URL (`localhost:8081`) is for
Docker Flink metrics. It can be unavailable during an IntelliJ run while the
Kafka dashboard views still work.

## Topic names must match

The saved configurations already use these names:

| Purpose | Topic |
|---|---|
| Producer output / Flink input | `github-users-multi1` |
| Flink session output | `github-user-sessions-multi1` |
| Flink alert output | `github-user-alerts-multi1` |

New topics have **3 partitions**, and Flink defaults to **parallelism 3**.
The producer accepts `--partitions`; Java accepts `--partitions` and
`--parallelism`. User IDs remain Kafka keys, so each user's events stay together.
Existing one-partition topics keep their records and configuration. Use the new
`multi1` topics above; old records will not move into new partitions.

For a clean second run, stop the current producer and Java application and
choose fresh names, for example replace `multi1` with `multi2` in the Java,
producer, dashboard and consumer Run configurations. Then follow steps 2–5.
Kafka appends records, so repeatedly sending the same hour into the same active
job can cause duplicate input and events behind its existing watermark.

The dashboard's **Connection settings** can switch its observed topics and
choose earliest or latest. This only resets the dashboard view; it does not
start the producer, change Flink's offsets or delete Kafka data.

## Stop the project

Stop the producer, Java application, dashboard and consumers with their IDE
Stop buttons. Stop Kafka from the project root with:

```powershell
docker compose -f python/docker-compose.yml stop
```

This keeps the Kafka container for later use. `docker compose down` or the
course `python/stop.sh` removes it and its container-stored Kafka records.

## Common problems

- **Docker engine unavailable:** open Docker Desktop and wait until it is running.
- **Port 9092 already in use:** run `docker ps` and stop the previous Kafka stack before starting this one. If the old homework containers are running, finish their jobs and use:

  ```powershell
  docker stop hw1-231035-taskmanager-1 hw1-231035-jobmanager-1 hw1-231035-kafka-1
  ```

- **NoBrokersAvailable:** check that Kafka is healthy and that IDE applications use `localhost:9092`. `kafka:9093` is for containers on Kafka's Docker network.
- **Wrong Python packages:** select this project's `python/.venv` interpreter, install `python/requirements.txt`, and restart the Python Run configuration.
- **No output displayed:** verify that Java is still running and all topic names match. Select **Earliest** in dashboard settings to inspect existing output records.
- **Old records appear:** earliest includes retained history. Use fresh topics for a clean experiment.

## Optional: reproduce the earliest/latest comparison

This experiment uses Docker Flink. Run from the project root:

```powershell
docker compose -f python/docker-compose.yml up -d --wait
docker compose --profile processing build jobmanager
docker compose --profile processing up -d --wait jobmanager taskmanager
.\python\.venv\Scripts\python.exe python/compare_offsets.py --input "$env:USERPROFILE\Downloads\2026-10-15-15.json.gz"
```

The script creates fresh topics, preloads input before earliest, starts latest
before its producer, compares the JSON outputs, and measures pending results
before and after drain. Results are in [the comparison report](reports/offset-comparison.md)
and [JSON measurements](reports/offset-comparison.json).

## Documentation and submission

- [Python guide](python/README.md)
- [Java guide and class-exercise mapping](java/README.md#following-the-class-exercises)
- [Macedonian project documentation](output/pdf/project-documentation.pdf)

The Python base comes from [the course repository](https://github.com/stefanandonov/RNMP_homework1).
The Java application follows the supplied `flink-demo (1).zip` examples.
Flink is **2.3.0**, Java **17**, the Kafka broker **3.9.0**, and the Java Kafka
connector **5.0.0-2.2**, matching the exercise POM. Python uses `kafka-python==2.3.3`.

Submit the GitHub repository link on Moodle and give the teaching assistant access.
The repository includes the source code, configuration, PDF documentation and
comparison reports. Keep the dataset, virtual environments and build outputs out of Git.

The recorded comparison report describes the earlier single-partition run.
Re-run the comparison command to generate measurements for three partitions.

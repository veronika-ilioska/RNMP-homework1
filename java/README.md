# Java project for IntelliJ IDEA

Open this `java` folder in IntelliJ IDEA as a Maven project using `pom.xml`.
Use Java 17 for the project SDK and Maven importer/runner. The project uses Flink
2.3.0. Run `run_jobs.ps1` from this folder's terminal;
Docker supplies the build tools and Flink runtime, so a local Maven installation
is optional. The launcher starts Kafka from the course-based Python project
`../python`, then starts Flink using the parent Compose file.

The Python producer and dashboard are in `../python`, which opens as a
separate PyCharm project. See [the workspace guide](../README.md) for the complete
flow.

## Following the class exercises

The reference is the supplied `flink-demo (1).zip`, under
`flink-demo/src/main/java/mk/ukim/finki/rnmp/demo`. The homework applies its
patterns to GitHub users instead of webshop clicks:

| Exercise example | Homework implementation |
|---|---|
| `ClickEvent` | `GitHubEvent`: public POJO fields and a no-argument constructor |
| `Common` | `Common`: bootstrap address, Kafka source/deserializer, JSON sink and watermarks |
| `Step3EventTime` | `Common.watermarks`: bounded out-of-order timestamps from `event.timestamp` |
| `Step7Sessions` | `UserAnalytics`: `keyBy(userId)` and `EventTimeSessionWindows.withGap(30 minutes)` |
| `Step5RevenueStats` | `CountActivity` + `WindowResult`: incremental `AggregateFunction`, then window metadata |
| `Step6AbandonedCart` | `RepositoryInactivityDetector`: managed state, event-time timer registration/cancellation, `onTimer`, then state removal |
| Exercise `pom.xml` | Java 17, Flink 2.3.0, Kafka connector 5.0.0-2.2, automatic `ide` profile, IDE logging, shade and exec plugins |

`GitHubPipeline` follows the same `StreamExecutionEnvironment -> fromSource ->
keyBy -> window/process -> sinkTo -> execute` structure. Checkpointing runs every
10 seconds, as in Step 6. `--bootstrap` and `--ooo` are accepted as aliases for
`--bootstrap-servers` and `--watermark-seconds`, so the example argument style
works too. The default bootstrap is `localhost:9092`; the Docker launcher and
comparison explicitly pass `kafka:9093`.

The homework keeps the required extensions to those examples. The JSON
deserializer reads nested `actor`/`repo` fields. The sink also writes the user
Kafka key and event timestamp, so it uses `OutputRecord` rather than a plain
string. The watermark bound is 180 data seconds for the producer's 120-second
delay; idleness defaults to 10 wall-clock seconds for live processing across three partitions.
Offsets and bounded replay are arguments rather than the examples' fixed latest
offset and continuous input.

Step 6 uses one `ValueState` for a user's pending cart. Here `MapState` tracks
each repository while the Flink key stays the user ID. Events are buffered until
the watermark so out-of-order activity is handled in event-time order. Rescheduling
cancels an old timer only when no other repository deadline or buffered event
shares its timestamp: Flink deduplicates timers per user and timestamp.

## Run with IntelliJ's green Run button

Open this `java` folder, import/reload `pom.xml` as Maven, and choose **Java 17**
under **File > Project Structure > Project SDK**. Use Download JDK if Java 17 is
not already installed.

The `.run` directory supplies two Application configurations in the Run selector:

- **GitHub - Replay (both modules)** reads the already-produced
  `github-users-multi1` topic from earliest, flushes the final sessions, and finishes.
- **GitHub - Live (both modules)** waits for live records on `github-users-multi1`.
  Start this before **Producer - IDE live** in PyCharm.

Both run `mk.ukim.rnmp.GitHubPipeline` locally, connect to Kafka at
`localhost:9092`, and write to `github-user-sessions-multi1` and
`github-user-alerts-multi1`. Kafka from the course repo must already be healthy.
Run `docker compose up -d --wait` inside `python`. Flink runs inside
the IntelliJ process; submitting another Docker Flink job is unnecessary.
The exercise-style `ide` Maven profile activates in IntelliJ and includes the
Flink dependencies. The saved configurations also include `provided` dependencies
and Java 17 VM opens.
If a configuration reports an invalid module, select the imported Maven module
`github-pipeline` in **Run > Edit Configurations > Use classpath of module**.
Under **Modify options**, ensure **Add dependencies with provided scope to
classpath** is enabled.

View the outputs using PyCharm's saved consumer configurations. Edit **Program
arguments** to change the input topic or select just `--job analytics` or
`--job events`. Choose fresh input/output topic names for another clean run and
update the matching producer/consumer configurations. Ordinary Stop in IntelliJ
does not drain open sessions in live mode; use replay for a complete finite run.

## Analytics and event-driven modules

Both processing modules are Java 17 Flink DataStream jobs in this Maven project. `GitHubPipeline.java` is the entry point; `--job both` shares one Kafka
source between the two branches. The Python producer and inspection
scripts remain separate utilities.

```text
Python producer -> github-events -> Java Flink pipeline
                                    |-> github-user-sessions     (analytics)
                                    |-> github-user-alerts      (event-driven)
```

Both branches read the acting GitHub user's ID from `actor.id`, use the JSON
`timestamp` as event time, and call `keyBy(event -> event.userId)`. In this
archive, `actor.id` is the user who performed the event. Events for that user
are grouped across all repositories. The producer now uses `actor.id` as its
Kafka key. Java also derives the user from older messages whose Kafka keys were
repository IDs. Output Kafka keys are always user IDs.

The watermark allowance is 180 data seconds, accommodating the producer's
120-data-second delay. Start a fresh job with user result topics when switching
from repository grouping, because the keyed state and result schema changed.

### Run both Java modules

After producing the input, run from this directory in PowerShell:

```powershell
.\run_jobs.ps1
```

The script starts Kafka using the course Compose file in `../python`, discovers its
network, builds the Java application if the image is absent, starts the Flink
services on that network, and submits the JAR. Docker supplies Maven, Java 17,
Flink 2.3.0, and Kafka connector 5.0.0-2.2; local Maven is not required.
There is no published Kafka connector specifically for Flink 2.3 yet. The
2.2 connector is verified with this application on the 2.3.0 runtime; see the
[connector version note](../README.md#kafka-connector-version) and measured report.
After changing Java sources, rebuild with `.\run_jobs.ps1 -Build`.
If your PowerShell policy blocks local scripts, use this process-only invocation:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_jobs.ps1
```

Run either module independently or change its settings:

```powershell
.\run_jobs.ps1 -Job analytics -SessionGapMinutes 30 -AnalyticsTopic github-user-sessions
.\run_jobs.ps1 -Job events -QuietSeconds 1800 -EventsTopic github-user-alerts
```

`-StartOffset earliest` (default) starts at the first retained Kafka offset;
`-StartOffset latest` starts at the partition's end offset when initialized.
The Java application argument is `--start-offset earliest|latest`. These options
apply to a fresh job; a restored job resumes its saved source offsets.

`-Mode replay` reads up to each partition's latest offset captured at startup.
Execution uses streaming event-time operators. End of bounded input emits a
final watermark, flushes windows and buffered events, and lets the job finish.
With `latest` and `replay` on a preloaded topic, the starting offset equals the
stopping offset, so no historical data is read.

For continuous processing, start before producing new records:

```powershell
.\run_jobs.ps1 -Mode stream -StartOffset latest -Detach -InputTopic github-events-live -AnalyticsTopic github-user-activity-live -EventsTopic github-user-alerts-live
```

`-Detach` submits and returns; omit it to follow the running job. Flink's dashboard
is at http://localhost:8081. Use `docker compose -f ..\docker-compose.yml exec jobmanager flink list` and
`docker compose -f ..\docker-compose.yml exec jobmanager flink cancel <job-id>` to inspect/cancel jobs.
Interrupting an attached launcher may leave the submitted job running.

For a latest run, create an empty input topic before starting the job and wait
for Kafka partition initialization before starting the producer. Job status
`RUNNING` alone does not confirm that the latest starting offset was resolved.
The workspace's `python/compare_offsets.py` handles this synchronization and
uses new topics for every experiment. See the [measured report](../reports/offset-comparison.md).

### Analytics: user activity across repositories

`UserAnalytics.java` uses `EventTimeSessionWindows` with a **30-minute inactivity
gap** per user, an incremental `AggregateFunction`, and a `ProcessWindowFunction`
to attach session bounds. The gap is configurable with `-SessionGapMinutes` in
PowerShell or `--session-gap-minutes` in Java. Session length depends on activity.
Events at 15:00 and 15:20 produce a session `[15:00, 15:50)`; another event at
15:55 starts a new session because the gap from 15:20 exceeds 30 minutes.

An out-of-order event can bridge two still-open sessions; Flink then merges
their counts and distinct repository sets. Native Flink session windows also
merge intervals that touch, so events exactly 30 minutes apart merge if the
earlier session has not already closed. Each result describes one user's session:

| Field | Meaning |
|---|---|
| `user_id`, `user_login` | Acting user ID and login; greatest login observed in the window if it changed |
| `window_start`, `window_end` | UTC window bounds in ISO 8601 format |
| `window_type`, `session_gap_minutes` | `session` and the configured inactivity gap (default 30) |
| `event_count` | Total events |
| `unique_repositories` | Distinct repository IDs touched by this user during the window |
| `push_events` | Push events, not individual commits |
| `pull_request_events`, `issue_events` | Pull request and issue lifecycle events |
| `star_events`, `fork_events` | Watch/star events and fork events |
| `other_events` | Remaining types, including comments and reviews |

Results go to `github-user-sessions`; the Kafka record timestamp is session end
minus one millisecond. A session closes when the watermark reaches its end minus
one millisecond: 30 data minutes after the last event, plus the watermark allowance
as input time advances. Events whose resulting windows have already closed are
dropped by Flink's default window lateness policy. Start a fresh job and fresh
result topic when switching from tumbling windows; their saved state is incompatible.

### Event-driven: no repository activity for 30 minutes

`RepositoryInactivityDetector.java` is a `KeyedProcessFunction` keyed by user ID.
Its managed `MapState` stores separate last-activity timestamps, event counts and
deadlines for that user's repositories. Repository activity means any incoming
GitHub event with a repository ID. Acting in repository B does not extend the
deadline for repository A. Other users have separate state, even in the same repo.

Every processed event schedules a timer at its timestamp plus `QuietSeconds`
(default **1800 seconds = 30 data minutes**). When the watermark reaches the
current deadline, the timer directly emits `USER_REPOSITORY_INACTIVE` to the
notification topic. The user does not need to return for the alarm to fire.
For example, A's last event at 15:10 schedules A's alarm for 15:40, even if the
same user acts in B at 15:25.

The JSON includes `user_id`, `user_login`, `repository_id`, `repository_name`,
`last_activity_timestamp_ms`, `inactive_at_ms`, `quiet_period_ms`,
`threshold_seconds`, `events_before_inactivity`, and `finalized_by_end_of_input`.
The Kafka key remains the user ID; the record timestamp is the deadline.

Events are buffered until their event-time order is finalized by the watermark.
An event exactly at an open deadline extends that repository's activity before
the inactivity check. Superseded timers are ignored. Repositories sharing a
deadline are handled together. After emitting, the episode state is removed so
silence does not repeatedly produce the same alarm. A later event starts a new
activity episode. Events at or behind the watermark cannot change finalized
history and increment `events_behind_watermark`.

In stream mode, an idle input leaves timers pending until event time advances.
Replay and `stop --drain` emit a final watermark and fire the remaining deadlines;
those alarms have `finalized_by_end_of_input=true`. This identifies completion
from end of input, rather than 30 minutes of wall-clock observation. Start a
fresh job and fresh output topics when switching from the old resumption rule.

### Clean replay

Kafka topics are append-only. Replaying an old hour into the same input topic
makes event time jump backward, so watermarks may classify much of a second
copy as late. Re-running jobs also appends results. Use previously unused input
and output topic names for a clean run:

```powershell
..\python\.venv\Scripts\python.exe ..\python\produce_messages.py --input "$env:USERPROFILE\Downloads\2026-10-15-15.json.gz" --topic github-events-users --speed 60
.\run_jobs.ps1 -InputTopic github-events-users -AnalyticsTopic github-user-activity-java -EventsTopic github-user-alerts-java
```

The supplied file has 3,582 events from 2,474 users. The recorded comparison
contains the session and per-repository inactivity counts for the current rule.
Recorded comparison results and Kafka topic names are in the
[comparison report](../reports/offset-comparison.md).

Inspect either result topic using the existing Python consumer:

```powershell
..\python\.venv\Scripts\python.exe ..\python\consume_messages.py github-user-sessions
..\python\.venv\Scripts\python.exe ..\python\consume_messages.py github-user-alerts
```

### Build and execution details

`pom.xml` can be opened directly in IntelliJ IDEA. With local Maven and
Java 17, run `mvn package`. Docker builds package the shaded application JAR:

```powershell
docker compose -f ..\docker-compose.yml --profile processing build jobmanager
```

For a native Maven run, use Java 17 and the `ide` profile (from this folder):

```powershell
$env:MAVEN_OPTS = '--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED'
mvn -Pide compile exec:java "-Dexec.args=--job both --mode stream --start-offset earliest --bootstrap localhost:9092 --input-topic github-users-multi1 --analytics-topic github-user-sessions-multi1 --events-topic github-user-alerts-multi1 --ooo 180"
```

Use the default profile for a cluster JAR. Logging dependencies are available
locally and excluded from the shaded JAR, matching the exercise POM. Jackson
remains relocated to avoid a conflict with the cluster's libraries;
`flink-connector-base` is packaged because the Docker runtime needs it.

To submit the packaged Java application directly:

```powershell
docker compose -f ..\docker-compose.yml exec -T jobmanager flink run /opt/flink/usrlib/github-pipeline.jar --job both --mode replay --start-offset earliest --session-gap-minutes 30 --input-topic github-events --analytics-topic github-user-sessions --events-topic github-user-alerts
```

All timer, watermark, and window durations use **data time**, independent of
producer speed. Streaming mode needs advancing watermarks: a finite source that
stops producing leaves final windows and buffered tail events pending. Replay
mode closes them at end of input. Live processing defaults to three partitions,
parallelism 3 and `--idle-seconds 10`, so an inactive partition can be excluded
from watermark computation. Idleness does not create new event time. The
controlled offset experiment explicitly uses parallelism 1 and idleness 0 with
three partitions to keep the no-data observation and metrics comparable.

After a continuous job has consumed the final record, finish it with a final
watermark using `stop --drain`. This closes the pending windows and fires the
pending event-time timers. The savepoint path is shared by both Flink containers:

```powershell
docker compose -f ..\docker-compose.yml exec -T jobmanager flink stop --drain --savepointPath file:///opt/flink/savepoints <job-id>
```

Use drain for final termination; an ordinary cancellation does not flush the
tail. Repository inactivity timers emit their alarms directly. Alarms emitted
at the final watermark are marked `finalized_by_end_of_input=true`.

Checkpoints run every ten seconds and Kafka delivery is at least once. This local deployment does not
configure durable checkpoint storage or automatic recovery after deleting its
containers. Failures and reruns can produce duplicates. User histories
remain in managed state for the job lifetime; they are not evicted automatically.
The default parallelism is 3 for the three-partition topics; the TaskManager
has four slots for experiments and concurrent jobs.

The Flink services use the `processing` Compose profile. Kafka is managed
separately by the class repo. To stop Flink, then course Kafka:

```powershell
docker compose -f ..\docker-compose.yml --profile processing down
docker compose -f ..\python\docker-compose.yml down
```

Class wiring: https://github.com/stefanandonov/RNMP_homework1
Flink process functions: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/

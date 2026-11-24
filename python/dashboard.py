"""Local, read-only Kafka observatory for the Java Flink homework pipeline."""

import argparse
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
import threading
import time
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import urlopen

from kafka import KafkaConsumer


ASSETS = Path(__file__).with_name("dashboard")
MAX_EVENTS = 20_000


@dataclass(frozen=True)
class Connection:
    bootstrap_servers: str = "localhost:9092"
    input_topic: str = "github-users-repo1"
    analytics_topic: str = "github-user-sessions-repo1"
    events_topic: str = "github-user-alerts-repo1"
    start_offset: str = "earliest"
    flink_url: str = "http://localhost:8081"


def epoch(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int((parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp() * 1000)


def matches(row, query):
    return not query or query in (row["key"] + " " + str(row["value"].get("user_login", ""))
                                 + " " + str(row["value"].get("actor", {}).get("login", ""))).lower()


class Store:
    def __init__(self, connection):
        self.connection = connection
        self.lock = threading.RLock()
        self.rows = {"input": deque(maxlen=MAX_EVENTS), "analytics": deque(maxlen=10_000),
                     "events": deque(maxlen=2_000)}
        self.counts = Counter()
        self.types = Counter()
        self.max_timestamp = None
        self.out_of_order = 0
        self.invalid = 0
        self.status = "connecting"
        self.error = None
        self.offsets = {}
        self.rates = deque(maxlen=90)
        self.flink = {"available": False, "message": "Looking for a matching Flink job"}
        self.last_offsets = {}

    def ingest(self, message):
        branch = {self.connection.input_topic: "input", self.connection.analytics_topic: "analytics",
                  self.connection.events_topic: "events"}[message.topic]
        partition = (message.topic, message.partition)
        with self.lock:
            # Retrying a broker connection must not count the same Kafka offset twice.
            if message.offset <= self.last_offsets.get(partition, -1):
                return
            self.last_offsets[partition] = message.offset
        try:
            value = json.loads(message.value)
            if not isinstance(value, dict):
                raise ValueError("Expected a JSON object")
            key = message.key.decode("utf-8") if message.key is not None else ""
            # Keep event summaries, not potentially very large GitHub payloads.
            if branch == "input":
                timestamp = value.get("timestamp", message.timestamp)
                if not isinstance(timestamp, int):
                    raise ValueError("Expected an integer event timestamp")
                if not isinstance(value.get("actor"), dict) or not isinstance(value.get("repo", {}), dict):
                    raise ValueError("Expected GitHub actor and repository objects")
                value = {name: value[name] for name in ("id", "type", "actor", "repo", "created_at", "timestamp")
                         if name in value}
            else:
                timestamp = message.timestamp
                if branch == "events" and value.get("alert_type") != "USER_REPOSITORY_INACTIVE":
                    raise ValueError("Choose a repository-inactivity notification topic")
                if branch == "analytics":
                    if value.get("window_type") != "session":
                        raise ValueError("Expected a Flink session result; choose the session output topic")
                    epoch(value["window_start"])
                    epoch(value["window_end"])
            row = {"key": key, "partition": message.partition, "offset": message.offset,
                   "timestamp": timestamp, "value": value, "observed_at": int(time.time() * 1000)}
            with self.lock:
                self.rows[branch].append(row)
                self.counts[branch] += 1
                if branch == "input":
                    self.types[value.get("type", "Unknown")] += 1
                    if self.max_timestamp is not None and timestamp < self.max_timestamp:
                        self.out_of_order += 1
                    self.max_timestamp = max(timestamp, self.max_timestamp or timestamp)
        except (ValueError, TypeError, KeyError, UnicodeError) as error:
            with self.lock:
                self.invalid += 1
                self.error = f"Skipped invalid JSON record: {error}"

    def snapshot(self, query=""):
        with self.lock:
            rows = {name: [row for row in values if matches(row, query)] for name, values in self.rows.items()}
            # Preview is deliberately independent of Flink and explicitly labelled in the UI.
            histories = defaultdict(list)
            for row in rows["input"]:
                histories[row["key"]].append(row)
            confirmed = {(row["key"], epoch(row["value"]["window_start"])): row["value"]
                         for row in rows["analytics"]}
            previews = []
            top_users = sorted(histories, key=lambda key: len(histories[key]), reverse=True)[:12]
            for key in top_users:
                current = None
                for row in sorted(histories[key], key=lambda item: item["timestamp"]):
                    timestamp = row["timestamp"]
                    if current is None or timestamp > current["end"]:
                        current = {"key": key, "login": row["value"].get("actor", {}).get("login", key),
                                   "start": timestamp, "end": timestamp + 1_800_000,
                                   "event_count": 0, "timestamps": []}
                        previews.append(current)
                    current["end"] = timestamp + 1_800_000
                    current["event_count"] += 1
                    if len(current["timestamps"]) < 200:
                        current["timestamps"].append(timestamp)
            for preview in previews:
                result = confirmed.get((preview["key"], preview["start"]))
                preview["confirmed"] = result is not None
                if result is not None:
                    # Solid intervals must use Flink's actual bounds/counts,
                    # even if a very late input changed the independent preview.
                    preview["end"] = epoch(result["window_end"])
                    preview["event_count"] = result["event_count"]
            return {"connection": asdict(self.connection), "status": self.status, "error": self.error,
                    "counts": dict(self.counts), "invalid_records": self.invalid,
                    "out_of_order": self.out_of_order, "max_event_timestamp": self.max_timestamp,
                    "offsets": self.offsets.copy(), "rates": list(self.rates),
                    "event_types": self.types.most_common(8), "flink": self.flink.copy(),
                    "rows": {"input": rows["input"][-150:], "analytics": rows["analytics"][-1000:],
                             "events": rows["events"][-150:]}, "previews": previews,
                    "retained_input_records": len(self.rows["input"]), "input_retention_limit": MAX_EVENTS,
                    "server_time": int(time.time() * 1000)}


class Observer:
    def __init__(self, connection):
        self.store = Store(connection)
        self.stop_event = threading.Event()
        self.threads = [threading.Thread(target=self.consume, daemon=True),
                        threading.Thread(target=self.flink_metrics, daemon=True)]

    def start(self):
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.stop_event.set()

    def consume(self):
        connection = self.store.connection
        while not self.stop_event.is_set():
            consumer = None
            try:
                consumer = KafkaConsumer(connection.input_topic, connection.analytics_topic, connection.events_topic,
                                         bootstrap_servers=connection.bootstrap_servers.split(","), group_id=None,
                                         enable_auto_commit=False, auto_offset_reset=connection.start_offset,
                                         receive_message_max_bytes=64 * 1024 * 1024,
                                         request_timeout_ms=15000, session_timeout_ms=10000)
                last_sample, previous_count = 0, 0
                while not self.stop_event.is_set():
                    batches = consumer.poll(timeout_ms=500, max_records=1000)
                    for batch in batches.values():
                        for message in batch:
                            self.store.ingest(message)
                    now = time.monotonic()
                    if now - last_sample < 2:
                        continue
                    assigned = list(consumer.assignment())
                    ends = consumer.end_offsets(assigned) if assigned else {}
                    starts = consumer.beginning_offsets(assigned) if assigned else {}
                    positions = {part: consumer.position(part) for part in assigned}
                    offsets = {}
                    for topic in (connection.input_topic, connection.analytics_topic, connection.events_topic):
                        parts = [part for part in assigned if part.topic == topic]
                        offsets[topic] = {"retained_records": sum(ends[p] - starts[p] for p in parts),
                                          "lag": sum(max(0, ends[p] - positions[p]) for p in parts),
                                          "partitions": [{"partition": p.partition, "begin": starts[p],
                                                          "end": ends[p], "position": positions[p]} for p in parts]}
                    with self.store.lock:
                        count = self.store.counts["input"]
                        rate = (count - previous_count) / (now - last_sample) if last_sample else 0
                        self.store.rates.append({"time": int(time.time() * 1000), "rate": round(rate, 2)})
                        self.store.offsets = offsets
                        self.store.status = "connected" if assigned else "connecting"
                        self.store.error = None
                    last_sample, previous_count = now, count
            except Exception as error:
                with self.store.lock:
                    self.store.status = "disconnected"
                    self.store.error = f"{type(error).__name__}: {error}"
                self.stop_event.wait(3)
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def flink_metrics(self):
        connection = self.store.connection

        def fetch(path):
            with urlopen(connection.flink_url.rstrip("/") + "/" + path, timeout=2) as response:
                return json.load(response)

        while not self.stop_event.is_set():
            try:
                jobs = fetch("jobs/overview")["jobs"]
                candidates = []
                for job in sorted(jobs, key=lambda item: item.get("start-time", 0), reverse=True)[:10]:
                    config = fetch(f"jobs/{job['jid']}/config")["execution-config"].get("user-config", {})
                    if config.get("input-topic") == connection.input_topic:
                        candidates.append(job)
                job = next((item for item in candidates if item["state"] == "RUNNING"),
                           candidates[0] if candidates else None)
                result = {"available": bool(job), "message": "No matching job at this Flink REST endpoint"}
                if job:
                    values = {}
                    for vertex in fetch(f"jobs/{job['jid']}").get("vertices", []):
                        path = f"jobs/{job['jid']}/vertices/{vertex['id']}/subtasks/0/metrics"
                        names = [entry["id"] for entry in fetch(path)]
                        selected = [name for name in names if name.rsplit(".", 1)[-1] in
                                    ("events_received", "events_finalized", "events_behind_watermark",
                                     "currentInputWatermark")]
                        if selected:
                            for item in fetch(path + "?" + urlencode({"get": ",".join(selected)})):
                                if item.get("value", "").lstrip("-").isdigit():
                                    values[item["id"].rsplit(".", 1)[-1]] = int(item["value"])
                    result.update(job_id=job["jid"], state=job["state"], metrics=values)
                with self.store.lock:
                    self.store.flink = result
            except Exception:
                with self.store.lock:
                    self.store.flink = {"available": False, "message": "Flink REST unavailable; Kafka views still work"}
            self.stop_event.wait(5)


class Dashboard:
    def __init__(self, connection):
        self.lock = threading.Lock()
        self.observer = Observer(connection)

    def connect(self, connection):
        with self.lock:
            self.observer.stop()
            self.observer = Observer(connection)
            self.observer.start()


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, dashboard, **kwargs):
        self.dashboard = dashboard
        super().__init__(*args, directory=str(ASSETS), **kwargs)

    def log_message(self, format, *args):
        logging.debug(format, *args)

    def json_response(self, value, status=200):
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        request = urlparse(self.path)
        if request.path == "/api/state":
            query = parse_qs(request.query).get("user", [""])[0].strip().lower()[:200]
            self.json_response(self.dashboard.observer.store.snapshot(query))
        elif request.path == "/api/report":
            path = Path(__file__).resolve().parent.parent / "reports" / "offset-comparison.json"
            self.json_response(json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {})
        elif request.path.startswith("/api/"):
            self.json_response({"error": "Unknown endpoint"}, 404)
        else:
            super().do_GET()

    def do_POST(self):
        if self.path != "/api/connect":
            self.json_response({"error": "Unknown endpoint"}, 404)
            return
        origin = self.headers.get("Origin")
        if origin and urlparse(origin).netloc != self.headers.get("Host"):
            self.json_response({"error": "Cross-origin requests are not allowed"}, 403)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 8192 or "application/json" not in self.headers.get("Content-Type", ""):
                raise ValueError("Expected a small JSON request")
            values = json.loads(self.rfile.read(length))
            if not isinstance(values, dict) or set(values) - set(asdict(Connection())):
                raise ValueError("Unknown connection option")
            connection = Connection(**values)
            topics = (connection.input_topic, connection.analytics_topic, connection.events_topic)
            if len(set(topics)) != 3 or any(not isinstance(t, str) or not t or len(t) > 249 or
                                           any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
                                               for c in t) or t in (".", "..") for t in topics):
                raise ValueError("Use three distinct valid Kafka topics")
            if connection.start_offset not in ("earliest", "latest"):
                raise ValueError("Offset must be earliest or latest")
            if not isinstance(connection.bootstrap_servers, str) or not connection.bootstrap_servers.strip():
                raise ValueError("Bootstrap servers are required")
            if not isinstance(connection.flink_url, str) or urlparse(connection.flink_url).scheme not in ("http", "https"):
                raise ValueError("Flink URL must use HTTP or HTTPS")
            self.dashboard.connect(connection)
            self.json_response({"ok": True})
        except (TypeError, ValueError) as error:
            self.json_response({"error": str(error)}, 400)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8501)
    for name, default in asdict(Connection()).items():
        parser.add_argument("--" + name.replace("_", "-"), default=default,
                            **({"choices": ["earliest", "latest"]} if name == "start_offset" else {}))
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)
    connection = Connection(**{key: getattr(args, key) for key in asdict(Connection())})
    dashboard = Dashboard(connection)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), partial(Handler, dashboard=dashboard))
    dashboard.observer.start()
    print(f"Dashboard: http://127.0.0.1:{args.port} (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.observer.stop()
        server.server_close()


if __name__ == "__main__":
    main()

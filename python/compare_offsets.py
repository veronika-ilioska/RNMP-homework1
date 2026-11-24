"""Run the earliest/latest Kafka experiment and save measured JSON/Markdown reports.

Requires the course Kafka in python/docker-compose.yml and this project's Flink services.
Experiment topics are single-partition. Both scenarios use fresh topics.
Only submitted experiment jobs drain.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid

from kafka.admin import KafkaAdminClient, NewTopic

from produce_messages import create_producer, load_events, prepare_topic, replay, schedule_events, send_event
from kafka_snapshot import read_snapshot


ROOT = Path(__file__).resolve().parent.parent
COUNTERS = ('events_received', 'events_finalized', 'events_behind_watermark',
            'inactivity_timers_fired', 'repository_inactivity_notifications')


class Experiment:
    def __init__(self, args):
        self.args = args
        self.prefix = 'user-offsets-' + datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:6]
        self.directory = args.report_dir / self.prefix
        self.directory.mkdir(parents=True)
        self.metric_paths = {}
        self.active_jobs = set()
        self.events = load_events(args.input)
        self.schedule = schedule_events(self.events, 0.1, 120, 42)

    def progress(self, message):
        print(message, flush=True)

    def cli(self, *arguments, log_name=None):
        result = subprocess.run(['docker', 'compose', *arguments], cwd=ROOT,
                                capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=180)
        output = result.stdout + result.stderr
        if log_name:
            (self.directory / log_name).write_text(output, encoding='utf-8')
        if result.returncode:
            raise RuntimeError(output[-12000:])
        return output

    def rest(self, path):
        with urllib.request.urlopen(self.args.flink_url.rstrip('/') + '/' + path, timeout=15) as response:
            return json.load(response)

    def wait(self, predicate, description, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.5)
        raise TimeoutError(description)

    def topics(self, case):
        return {name: f'{self.prefix}-{case}-{name}' for name in ('input', 'analytics', 'events')}

    def create_topics(self, topics):
        admin = KafkaAdminClient(bootstrap_servers=self.args.bootstrap_servers.split(','))
        try:
            admin.create_topics([NewTopic(topic, 1, 1, topic_configs={'retention.ms': '-1'})
                                 for topic in topics])
            def leaders_ready():
                metadata = admin.describe_topics(topics)
                return all(topic.get('partitions') and all(partition['leader'] >= 0
                           for partition in topic['partitions']) for topic in metadata)
            self.wait(leaders_ready, 'Kafka topic leaders did not become ready')
        finally:
            admin.close()

    def produce(self, topic):
        self.progress(f'Producing {len(self.events)} records to {topic} at speed {self.args.speed}x')
        started = time.monotonic()
        prepare_topic(self.args.bootstrap_servers, topic)
        producer = create_producer(self.args.bootstrap_servers)
        try:
            stats = replay(self.schedule, self.events[0].timestamp_ms, self.args.speed,
                           lambda event: send_event(producer, topic, event))
            producer.flush(timeout=30)
        finally:
            producer.close(timeout=10)
        return {'wall_seconds': round(time.monotonic() - started, 3), **stats}

    def submit(self, topics, start_offset, case):
        started = time.monotonic()
        output = self.cli('exec', '-T', 'jobmanager', 'flink', 'run', '-d',
                          '/opt/flink/usrlib/github-pipeline.jar', '--job', 'both', '--mode', 'stream',
                          '--start-offset', start_offset, '--input-topic', topics['input'],
                          '--bootstrap-servers', 'kafka:9093',
                          '--analytics-topic', topics['analytics'], '--events-topic', topics['events'],
                          '--session-gap-minutes', '30', '--watermark-seconds', '180', '--quiet-seconds', '1800',
                          '--idle-seconds', '0', '--group-id', self.prefix + '-' + case,
                          log_name=case + '-submit.log')
        match = re.search(r'JobID\s+([0-9a-f]{32})', output)
        if not match:
            raise RuntimeError('Could not find submitted job ID: ' + output)
        job_id = match.group(1)
        self.active_jobs.add(job_id)
        self.progress(f'{case}: job {job_id} submitted')
        return job_id, started

    def running(self, job_id):
        state = self.rest(f'jobs/{job_id}')['state']
        if state in ('FAILED', 'CANCELED', 'FINISHED'):
            raise RuntimeError(f'Job {job_id} reached {state} before expected readiness')
        return state == 'RUNNING'

    def metrics(self, job_id):
        if job_id not in self.metric_paths:
            details = self.rest(f'jobs/{job_id}')
            for vertex in details['vertices']:
                path = f"jobs/{job_id}/vertices/{vertex['id']}/subtasks/0/metrics"
                available = [metric['id'] for metric in self.rest(path)]
                selected = [name for name in available if name.rsplit('.', 1)[-1] in (*COUNTERS, 'currentInputWatermark')]
                if any(name.endswith('events_received') for name in selected):
                    self.metric_paths[job_id] = (path, selected)
                    break
            else:
                return None
        path, selected = self.metric_paths[job_id]
        query = urllib.parse.urlencode({'get': ','.join(selected)})
        values = self.rest(path + '?' + query)
        return {item['id'].rsplit('.', 1)[-1]: int(item['value']) for item in values}

    def wait_consumed(self, job_id):
        def consumed():
            values = self.metrics(job_id)
            if values and values.get('events_received', 0) > len(self.events):
                raise RuntimeError('Unexpected extra input records')
            return values if values and values.get('events_received') == len(self.events) else None
        return self.wait(consumed, 'The job did not consume all input records')

    def latest_ready(self, topics):
        partition = topics['input'] + '-0'
        def ready():
            logs = self.cli('logs', '--no-color', '--since', '5m', 'taskmanager')
            for line in logs.splitlines():
                if partition in line and ('offset=0' in line or 'offset 0' in line):
                    if 'Resetting offset' in line or 'Seeking to offset' in line:
                        return line
            return None
        evidence = self.wait(ready, 'Latest source did not confirm its starting position at offset zero')
        (self.directory / 'latest-source-ready.log').write_text(evidence + '\n', encoding='utf-8')
        return evidence

    def snapshot(self, topics):
        result = {}
        for branch in ('analytics', 'events'):
            records = read_snapshot(topics[branch], self.args.bootstrap_servers, 30)
            normalized = []
            for record in records:
                value = json.loads(record.value)
                key = record.key.decode('utf-8')
                if not isinstance(value, dict) or value['user_id'] != key:
                    raise ValueError('Expected JSON output with the acting user Kafka key')
                normalized.append(json.dumps({'key': key, 'timestamp_ms': record.timestamp, 'value': value},
                                             sort_keys=True, ensure_ascii=True, separators=(',', ':')))
            normalized.sort()
            result[branch] = {
                'count': len(records),
                'sha256': hashlib.sha256('\n'.join(normalized).encode('ascii')).hexdigest(),
                'rows': normalized,
            }
        return result

    def finish(self, job_id):
        def finished():
            details = self.rest(f'jobs/{job_id}')
            if details['state'] in ('FAILED', 'CANCELED'):
                raise RuntimeError(json.dumps(self.rest(f'jobs/{job_id}/exceptions')))
            return details if details['state'] == 'FINISHED' else None
        details = self.wait(finished, 'Job did not finish', timeout=180)
        self.active_jobs.discard(job_id)
        return details

    def stream_case(self, case, offset, topics, producer_info=None):
        job_id, started = self.submit(topics, offset, case)
        self.wait(lambda: self.running(job_id), 'Job did not become RUNNING')
        readiness = None
        if offset == 'latest':
            readiness = self.latest_ready(topics)
            self.progress('Latest source confirmed offset 0; starting the producer now')
            producer_info = self.produce(topics['input'])
        self.wait_consumed(job_id)
        caught_up = round(time.monotonic() - started, 3)
        time.sleep(3)
        first = self.snapshot(topics)
        before_idle = self.metrics(job_id)
        # Wall-clock silence is not an event-time end-of-input signal.
        # Keep the single-partition source active during this observation.
        time.sleep(10)
        partial = self.snapshot(topics)
        if any(first[branch]['rows'] != partial[branch]['rows'] for branch in first):
            raise AssertionError('Outputs did not stabilize during the no-data observation period')
        values = self.metrics(job_id)
        if values['events_received'] != len(self.events) or values['events_behind_watermark'] != 0:
            raise AssertionError(values)
        state_after_idle = self.rest(f'jobs/{job_id}')['state']
        if state_after_idle != 'RUNNING':
            raise AssertionError(f'Unbounded job unexpectedly reached {state_after_idle}')
        if values['currentInputWatermark'] != before_idle['currentInputWatermark']:
            raise AssertionError('Watermark unexpectedly advanced during the idle observation period')
        (self.directory / (case + '-idle.json')).write_text(json.dumps({
            'job_id': job_id, 'state': state_after_idle, 'metrics': values, 'producer': producer_info,
            'output_counts': {branch: partial[branch]['count'] for branch in partial},
            'seconds_from_submission_to_all_input_received': caught_up,
        }, indent=2), encoding='utf-8')
        self.progress(f"{case}: idle tail: analytics={partial['analytics']['count']}, events={partial['events']['count']}; draining")
        self.cli('exec', '-T', 'jobmanager', 'flink', 'stop', '--drain',
                 '--savepointPath', 'file:///opt/flink/savepoints', job_id, log_name=case + '-drain.log')
        details = self.finish(job_id)
        wall_seconds = round(time.monotonic() - started, 3)
        final = self.snapshot(topics)
        return {'start_offset': offset, 'mode': 'stream', 'topics': topics, 'job_id': job_id,
                'producer': producer_info, 'source_ready_evidence': readiness,
                'seconds_from_submission_to_all_input_received': caught_up,
                'flink_job_seconds_including_observation_and_drain': round(details['duration'] / 1000, 3),
                'wall_seconds_including_submission_observation_and_drain': wall_seconds,
                'state_after_producer_stopped': state_after_idle, 'metrics_before_drain': values,
                'before_drain': partial, 'after_drain': final,
                'extra_results_after_drain': {branch: final[branch]['count'] - partial[branch]['count'] for branch in final}}

    def run(self):
        flink_version = self.rest('overview')['flink-version']
        java_runtime = self.cli('exec', '-T', 'jobmanager', 'java', '-version').strip()
        earliest = self.topics('earliest')
        latest = self.topics('latest')
        report_topic = self.prefix + '-comparison'
        # The Java application creates its own output topics with retention.
        self.create_topics(sorted({earliest['input'], latest['input'], report_topic}))
        self.progress(f'Experiment {self.prefix}; fresh result topics')
        preload = self.produce(earliest['input'])
        first = self.stream_case('earliest', 'earliest', earliest, preload)
        second = self.stream_case('latest', 'latest', latest)
        for branch in ('analytics', 'events'):
            if first['after_drain'][branch]['rows'] != second['after_drain'][branch]['rows']:
                raise AssertionError('Earliest/latest complete results differ')
        input_hashes = {}
        for label, topics in (('earliest', earliest), ('latest', latest)):
            # Raw GitHub payloads are much larger than the aggregate results.
            records = read_snapshot(topics['input'], self.args.bootstrap_servers, 90)
            if len(records) != len(self.events):
                raise AssertionError('Input topic record count differs from the file')
            digest = hashlib.sha256()
            for record in records:
                digest.update(record.key + b'\0' + str(record.timestamp).encode() + b'\0' + record.value + b'\n')
            input_hashes[label] = digest.hexdigest()
        if input_hashes['earliest'] != input_hashes['latest']:
            raise AssertionError('The two input topics contain different messages or orders')
        report = {'run_id': self.prefix, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
                  'flink_version': flink_version, 'java_runtime': java_runtime,
                  'kafka_source_repository': 'https://github.com/stefanandonov/RNMP_homework1',
                  'kafka_bootstrap_servers': self.args.bootstrap_servers,
                  'input_file': str(self.args.input), 'input_records': len(self.events), 'key_field': 'actor.id',
                  'input_topic_sha256': input_hashes, 'speed': self.args.speed,
                  'watermark_seconds': 180, 'idle_seconds': 0, 'window_type': 'session',
                  'session_gap_minutes': 30, 'quiet_seconds': 1800, 'event_rule': 'USER_REPOSITORY_INACTIVE',
                  'earliest': first, 'latest': second, 'complete_results_equal': True,
                  'partial_results_equal': all(first['before_drain'][branch]['rows'] == second['before_drain'][branch]['rows']
                                               for branch in ('analytics', 'events')),
                  'all_output_values_valid_json': True, 'comparison_topic': report_topic}
        # Keep hashes and counts in the compact report; exact row comparisons
        # above have already compared every result between offset scenarios.
        def compact(value):
            if isinstance(value, dict):
                return {key: compact(item) for key, item in value.items() if key != 'rows'}
            return value
        report = compact(report)
        self.args.report_dir.mkdir(parents=True, exist_ok=True)
        for path in (self.directory / 'comparison.json', self.args.report_dir / 'offset-comparison.json'):
            path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        publisher = create_producer(self.args.bootstrap_servers)
        try:
            publisher.send(report_topic, key=self.prefix, value=report).get(timeout=30)
            publisher.flush(timeout=30)
        finally:
            publisher.close(timeout=10)
        self.progress('PASS: input bytes/order, JSON values, keys, timestamps, all summaries and all notifications match')
        markdown = render_report(report)
        (self.args.report_dir / 'offset-comparison.md').write_text(markdown, encoding='utf-8')
        (self.directory / 'comparison.md').write_text(
            markdown.replace('](offset-comparison.json)', '](comparison.json)'), encoding='utf-8')
        self.progress(f'Report: {self.args.report_dir / "offset-comparison.json"}')
        return report


def render_report(report):
    first, second = report['earliest'], report['latest']
    complete = first['after_drain']
    earliest_producer_seconds = (f"{first['producer']['wall_seconds']:.3f}" if first['producer'] else 'Пред мерењето')
    pending = lambda run: (run['metrics_before_drain']['events_received']
                           - run['metrics_before_drain']['events_finalized'])
    watermark = datetime.fromtimestamp(second['metrics_before_drain']['currentInputWatermark'] / 1000,
                                       timezone.utc).isoformat(timespec='milliseconds')
    return f'''# 5–6. Kafka JSON излези и споредба на earliest/latest

Групирањето е по корисникот што го извршил настанот, преку `actor.id`.
Настаните од различни repositories за истиот корисник делат прозорци и состојба.

Извршување: `{report['run_id']}`. Двете влезни теми содржеа истите **{report['input_records']:,}**
записи во ист редослед. Тоа е потврдено со SHA-256 на Kafka клучевите,
временските ознаки и вредностите. Користени се брзина {report['speed']:g}×,
session прозорци со пауза од 30 минути, watermark од 180 секунди и праг на неактивност по repository од 1800 секунди.
Producer-от одложи 10% од записите за 120 секунди во време на настаните.
Flink runtime: **{report['flink_version']}**. Java runtime: `{report['java_runtime'].splitlines()[0]}`.
Kafka е стартуван од [RNMP_homework1]({report['kafka_source_repository']});
мерениот bootstrap endpoint е `{report['kafka_bootstrap_servers']}`.

- **Earliest:** producer-от заврши пред да се стартува Flink. Изворот читаше
  од првиот достапен offset.
- **Latest:** Flink беше стартуван на празна тема. Kafka клиентот потврди дека
  почнува од offset 0 пред да се стартува producer-от. Споредбата не се потпира
  само на статусот `RUNNING` за да ја утврди подготвеноста на изворот.

| Мерење | Earliest | Latest |
|---|---:|---:|
| Време на producer-от (s) | {earliest_producer_seconds} | {second['producer']['wall_seconds']:.3f} |
| Од поднесување до потврден прием на сите записи (s) | {first['seconds_from_submission_to_all_input_received']:.3f} | {second['seconds_from_submission_to_all_input_received']:.3f} |
| Траење на Flink job до завршување со drain (s) | {first['flink_job_seconds_including_observation_and_drain']:.3f} | {second['flink_job_seconds_including_observation_and_drain']:.3f} |
| Аналитички резултати пред drain | {first['before_drain']['analytics']['count']:,} | {second['before_drain']['analytics']['count']:,} |
| Известувања пред drain | {first['before_drain']['events']['count']:,} | {second['before_drain']['events']['count']:,} |
| Настани што чекаат watermark пред drain | {pending(first):,} | {pending(second):,} |
| Активирани тајмери за неактивност пред drain | {first['metrics_before_drain']['inactivity_timers_fired']:,} | {second['metrics_before_drain']['inactivity_timers_fired']:,} |
| Аналитички резултати по drain | {complete['analytics']['count']:,} | {second['after_drain']['analytics']['count']:,} |
| Известувања по drain | {complete['events']['count']:,} | {second['after_drain']['events']['count']:,} |

Траењето на Flink job вклучува намерно набљудување по приемот и завршување
со `stop --drain`. Кај earliest, претходното полнење на Kafka не е дел од
траењето на Flink; кај latest, job-от чека producer-от да ја репродуцира датотеката.
Затоа ова се мерења на целите сценарија, а не споредба на брзината на операторите.

**Резултатите се исти по drain.** Споредени се сите JSON вредности, Kafka
клучеви и временски ознаки, независно од редоследот на излезите. И двата
извора ги прочитаа истите записи со исти поставки за event time, watermark,
агрегации и тајмери.

**Крај на податоците.** Кога producer-от заврши, двата неограничени job-а останаа
`RUNNING`. И по 10 секунди без нови податоци, watermark остана `{watermark}` и
излезите не се променија. Отворените сесии чекаат watermark до 30 минути по последниот настан на корисникот.
Настаните понови од watermark и идните event-time тајмери исто така чекаат. Ѕидниот часовник
и idleness сами по себе не значат крај на влезот.

Автоматското означување на изворот како idle е исклучено (`--idle-seconds 0`).
Така единствената партиција останува активна за набљудувањето на watermark-от.
Idleness исклучува тивки партиции од пресметката; не создава ново event time.

`stop --drain` испрати финален `MAX_WATERMARK`, ги затвори преостанатите прозорци
и ги активира преостанатите event-time тајмери. Тајмерот емитува
`USER_REPOSITORY_INACTIVE` без да чека нов настан. Алармите затворени со
финалниот watermark имаат `finalized_by_end_of_input=true`: нивниот deadline
е финализиран со крајот на влезот, а не со 30 минути реално чекање.

**Kafka JSON излези.** Аналитиката и известувањата се во одделни теми, со
истиот user Kafka клуч (`actor.id`). Измерената споредба е исто објавена како една
JSON порака во `{report['comparison_topic']}`. Точните имиња на темите,
job идентификаторите и SHA-256 вредностите се во [JSON извештајот](offset-comparison.json).

Референци: [Flink Kafka source](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kafka/),
[Flink stop with drain](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/cli/#stopping-a-job-gracefully-creating-a-final-savepoint).
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--speed', type=float, default=60)
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--flink-url', default='http://localhost:8081')
    parser.add_argument('--report-dir', type=Path, default=ROOT / 'reports')
    args = parser.parse_args()
    args.input = args.input.resolve()
    if not 0 < args.speed < float('inf'):
        parser.error('Speed must be finite and positive')
    experiment = Experiment(args)
    try:
        experiment.run()
    finally:
        # Do not leave an experiment job running after a failed comparison.
        for job_id in list(experiment.active_jobs):
            try:
                experiment.cli('exec', '-T', 'jobmanager', 'flink', 'cancel', job_id,
                               log_name=job_id + '-cleanup.log')
            except Exception as error:
                print(f'Could not cancel experiment job {job_id}: {error}', file=sys.stderr)


if __name__ == '__main__':
    main()

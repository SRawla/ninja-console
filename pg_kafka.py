#!/usr/bin/env python3
"""
pg_kafka.py — Kafka engine for Ninja Console's multi-engine console.

Kafka is a broker, not a database, so this is a READ-ONLY message browser: the
schema tree lists topics, and "querying" a topic consumes its most recent
messages into the JSON document viewer (partition / offset / timestamp / key /
value). It never produces/writes.

Query input is a topic name, or a JSON spec:
  <topic-name>
  {"topic":"<name>", "limit":50, "from":"latest"|"earliest", "partition":0}

Common engine interface: connect / run / tables / columns / classify / version /
close. kafka-python is imported lazily (optional).
"""
import json
import time

CONNECT_TIMEOUT = 8
CONSUME_TIMEOUT = 8
DEFAULT_LIMIT = 50

ENGINE = 'kafka'
DIALECT = 'kafka'


def _driver():
    try:
        from kafka import KafkaConsumer, TopicPartition
        return KafkaConsumer, TopicPartition
    except ImportError as e:
        raise RuntimeError("Kafka support needs the kafka-python package:  pip install kafka-python") from e


def connect(info):
    KafkaConsumer, _ = _driver()
    host = info.get('host', 'localhost')
    port = int(info.get('port') or info.get('localPort') or 9092)
    kw = dict(bootstrap_servers=f'{host}:{port}', enable_auto_commit=False, group_id=None,
              request_timeout_ms=CONNECT_TIMEOUT * 1000 + 1000,
              consumer_timeout_ms=CONSUME_TIMEOUT * 1000)
    if info.get('username'):          # optional SASL (prod)
        kw.update(security_protocol=info.get('security_protocol', 'SASL_PLAINTEXT'),
                  sasl_mechanism=info.get('sasl_mechanism', 'PLAIN'),
                  sasl_plain_username=info['username'],
                  sasl_plain_password=info.get('password', ''))
    consumer = KafkaConsumer(**kw)
    try:
        consumer.topics()             # probe — also surfaces the advertised-listener problem early
    except Exception as e:
        try:
            consumer.close()
        except Exception:
            pass
        raise RuntimeError(
            "reached the bootstrap port, but the broker advertises internal addresses "
            "that aren't reachable through the port-forward (Kafka advertised.listeners). "
            "Kafka browsing works only when the broker advertises a locally-reachable host. "
            "[" + str(e)[:120] + "]")
    return {'consumer': consumer, 'bootstrap': f'{host}:{port}'}


def _decode(v):
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode('utf-8')
        except Exception:
            return v.hex()
    if isinstance(v, str):
        try:
            return json.loads(v)      # pretty-render JSON payloads
        except Exception:
            return v
    return v


def run(handle, text, info):
    _, TopicPartition = _driver()
    text = (text or '').strip()
    topic, limit, frm, part = None, DEFAULT_LIMIT, 'latest', None
    if text.startswith('{'):
        s = json.loads(text)
        topic, limit = s.get('topic'), int(s.get('limit', DEFAULT_LIMIT))
        frm, part = s.get('from', 'latest'), s.get('partition')
    else:
        topic = text.split()[0] if text else None
    if not topic:
        raise RuntimeError('specify a topic:  <topic-name>  or  {"topic":"...","limit":50}')

    consumer = handle['consumer']
    parts = consumer.partitions_for_topic(topic)
    if not parts:
        raise RuntimeError(f'topic not found (or no partitions): {topic}')
    tps = [TopicPartition(topic, int(part))] if part is not None else \
          [TopicPartition(topic, p) for p in parts]
    consumer.assign(tps)
    begin = consumer.beginning_offsets(tps)
    end = consumer.end_offsets(tps)
    per = max(1, limit // len(tps))
    for tp in tps:
        consumer.seek(tp, begin[tp] if frm == 'earliest' else max(begin[tp], end[tp] - per))

    msgs, deadline = [], time.time() + CONSUME_TIMEOUT
    while len(msgs) < limit and time.time() < deadline:
        polled = consumer.poll(timeout_ms=700, max_records=limit)
        if not polled:
            break
        for tp, records in polled.items():
            for r in records:
                msgs.append({'partition': r.partition, 'offset': r.offset,
                             'timestamp': r.timestamp, 'key': _decode(r.key), 'value': _decode(r.value)})
    consumer.unsubscribe()
    msgs.sort(key=lambda m: m['timestamp'], reverse=True)
    return {'result_kind': 'docs', 'docs': msgs[:limit]}


def tables(handle, info):
    tops = handle['consumer'].topics()
    return sorted([{'schema': 'topics', 'table': t, 'kind': 'topic'}
                   for t in tops if not t.startswith('__')], key=lambda x: x['table'])


def columns(handle, info, schema, table):
    return []


def classify(text):
    return 'read'                     # consume-only; this engine never writes


def version(handle):
    return 'Kafka'


def close(handle):
    try:
        handle['consumer'].close()
    except Exception:
        pass

"""Kafka client compatibility suite for BigPipe.

Runs real Kafka clients against a running node:
  * librdkafka (confluent-kafka): idempotent producer, every compression codec,
    consumer group with commit, AdminClient.
  * kafka-python: producer, consumer group, offset resume.

Usage:  pip install confluent-kafka kafka-python
        python tests/compat/python_compat.py [bootstrap]
"""
import sys
import time
import uuid

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


def librdkafka_suite():
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = f"compat-rd-{uuid.uuid4().hex[:6]}"
    fs = admin.create_topics([NewTopic(topic, num_partitions=4, replication_factor=1)])
    try:
        fs[topic].result(10)
        check("admin.create_topics", True)
    except Exception as e:  # noqa: BLE001
        check("admin.create_topics", False, repr(e))
    md = admin.list_topics(timeout=10)
    check("admin.list_topics", topic in md.topics and len(md.topics[topic].partitions) == 4)

    res = ConfigResource(ConfigResource.Type.TOPIC, topic)
    cfg = admin.describe_configs([res])[res].result(10)
    check("admin.describe_configs", "bigpipe.storage.mode" in cfg, cfg.get("bigpipe.storage.mode").value if "bigpipe.storage.mode" in cfg else "")

    total = 0
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"]:
        p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "compression.type": codec, "linger.ms": 5})
        errors = []
        delivered = []

        def cb(err, msg):
            (errors if err else delivered).append(err or msg.offset())

        for i in range(500):
            p.produce(topic, key=f"k{i}", value=f'{{"codec":"{codec}","n":{i}}}', headers={"codec": codec}, on_delivery=cb)
        p.flush(30)
        total += 500
        check(f"produce idempotent {codec}", not errors and len(delivered) == 500, f"errors={errors[:1]}")

    group = f"g-{uuid.uuid4().hex[:6]}"
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    seen = 0
    codecs = set()
    deadline = time.time() + 40
    while seen < total and time.time() < deadline:
        msgs = c.consume(500, timeout=1.0)
        for m in msgs:
            if m.error():
                continue
            seen += 1
            codecs.add(dict(m.headers() or [])["codec"].decode())
        if msgs:
            c.commit(asynchronous=False)
    check("consumer group read all", seen == total, f"{seen}/{total}")
    check("headers roundtrip", codecs == {"none", "gzip", "snappy", "lz4", "zstd"})
    committed = c.committed([TopicPartition(topic, p) for p in range(4)], timeout=10)
    check("offset commit/fetch", sum(tp.offset for tp in committed) == total, str([tp.offset for tp in committed]))
    c.close()

    # Timestamp lookup and resume from committed offsets
    c2 = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest"})
    lo, hi = c2.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    check("watermarks", lo == 0 and hi > 0, f"{lo}..{hi}")
    tps = c2.offsets_for_times([TopicPartition(topic, 0, int(time.time() * 1000) - 60_000)], timeout=10)
    check("offsets_for_times", tps[0].offset == 0, str(tps[0].offset))
    c2.subscribe([topic])
    extra = c2.consume(10, timeout=5)
    check("resume from committed (no redelivery)", len([m for m in extra if not m.error()]) == 0)
    c2.close()

    fut = admin.list_groups(timeout=10) if hasattr(admin, "list_groups") else None
    if fut is not None:
        check("admin.list_groups", any(g.id == group for g in fut))
    return topic


def kafka_python_suite():
    from kafka import KafkaConsumer, KafkaProducer

    topic = f"compat-kp-{uuid.uuid4().hex[:6]}"
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all")
    futs = [p.send(topic, key=f"k{i}".encode(), value=f"v{i}".encode()) for i in range(300)]
    p.flush()
    ok = all(f.get(timeout=10) for f in futs)
    check("kafka-python produce", ok)
    c = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id=f"kp-{uuid.uuid4().hex[:6]}",
                      auto_offset_reset="earliest", consumer_timeout_ms=8000)
    n = sum(1 for _ in c)
    check("kafka-python consumer group", n == 300, f"{n}/300")
    c.close()


if __name__ == "__main__":
    librdkafka_suite()
    kafka_python_suite()
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)

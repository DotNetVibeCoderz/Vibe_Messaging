"""End-to-end checks for BigPipe-specific features against a running node.

Covers diskless and tiered storage, online mode migration, share groups with DLQ, flows,
consumer-group rebalancing across two librdkafka consumers, SSE streaming and HTTP groups.

Usage:  pip install confluent-kafka
        python tests/integration/test_features.py [bootstrap] [http] [admin]
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

BOOT = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
HTTP = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8082"
ADMIN = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:9644"
results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)


def call(method, url, body=None, headers=None):
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    h = {"content-type": "application/json"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def produce_kafka(topic, n, prefix="v"):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": BOOT, "enable.idempotence": True, "linger.ms": 5})
    lat = []
    errs = []
    for i in range(n):
        t0 = time.perf_counter()
        p.produce(topic, key=f"k{i}", value=f"{prefix}{i}",
                  on_delivery=lambda e, m, t0=t0: (errs.append(e) if e else lat.append(time.perf_counter() - t0)))
    p.flush(60)
    return errs, sorted(lat)


def consume_all(topic, expected, timeout=40):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOT, "group.id": uid("g"), "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    got = []
    deadline = time.time() + timeout
    while len(got) < expected and time.time() < deadline:
        for m in c.consume(1000, 1.0):
            if not m.error():
                got.append((m.partition(), m.offset(), m.value().decode()))
    c.close()
    return got


def test_diskless():
    t = uid("dl")
    s, _ = call("POST", f"{ADMIN}/v1/topics", {"name": t, "partitions": 3, "mode": "diskless"})
    check("create diskless topic", s == 201)
    errs, lat = produce_kafka(t, 2000)
    p99 = lat[int(len(lat) * 0.99)] * 1000 if lat else -1
    check("diskless produce via Kafka protocol", not errs, f"p50={lat[len(lat) // 2] * 1000:.0f}ms p99={p99:.0f}ms")
    got = consume_all(t, 2000)
    check("diskless consume", len(got) == 2000, f"{len(got)}/2000")
    _, info = call("GET", f"{ADMIN}/v1/topics/{t}")
    check("diskless bytes live in object storage", info["bytes"]["diskless"] > 0 and info["bytes"]["local"] == 0, str(info["bytes"]))


def test_migration():
    t = uid("mig")
    call("POST", f"{ADMIN}/v1/topics", {"name": t, "partitions": 1})
    produce_kafka(t, 100, "a")
    s, r = call("POST", f"{ADMIN}/v1/topics/{t}/migrate", {"to": "diskless"})
    check("migrate local -> diskless", s == 200 and r["switch_offsets"]["0"] == 100, str(r))
    produce_kafka(t, 100, "b")
    call("POST", f"{ADMIN}/v1/topics/{t}/migrate", {"to": "local"})
    produce_kafka(t, 100, "c")
    got = consume_all(t, 300)
    offsets = [o for _, o, _ in got]
    ok = offsets == list(range(300)) and got[0][2] == "a0" and got[150][2] == "b50" and got[299][2] == "c99"
    check("offsets contiguous across local/diskless/local", ok, f"n={len(got)}")


def test_tiered():
    t = uid("tier")
    call("POST", f"{ADMIN}/v1/topics", {"name": t, "partitions": 1, "mode": "tiered",
                                         "config": {"segment.bytes": 4096, "local.retention.ms": 0, "bigpipe.cache.bytes": 0}})
    produce_kafka(t, 400)
    deadline = time.time() + 20
    info = {}
    while time.time() < deadline:
        _, info = call("GET", f"{ADMIN}/v1/topics/{t}")
        d = info["partition_details"][0]
        if d["remote_segments"] > 0 and d["local_bytes"] < info["bytes"]["remote"]:
            break
        time.sleep(0.5)
    d = info["partition_details"][0]
    check("tiered segments uploaded and evicted locally", d["remote_segments"] > 0,
          f"remote_segments={d['remote_segments']} local={d['local_bytes']} remote={d['remote_bytes']}")
    got = consume_all(t, 400)
    check("read back from object storage", len(got) == 400 and [o for _, o, _ in got] == list(range(400)), f"{len(got)}/400")


def test_share_groups():
    t = uid("jobs")
    dlq = t + ".dlq"
    call("POST", f"{ADMIN}/v1/topics", {"name": t, "partitions": 2})
    call("POST", f"{HTTP}/v1/topics/{t}/records", {"records": [{"value": f"job-{i}"} for i in range(10)]})
    g = uid("workers")
    _, a = call("POST", f"{HTTP}/v1/share/{g}/poll",
                {"member": "A", "topics": [t], "max_records": 5, "lock_ms": 60000, "max_attempts": 3, "dlq_topic": dlq})
    recs = a["records"]
    check("share poll acquires records", len(recs) == 5)
    acks = [{"topic": r["topic"], "partition": r["partition"], "offset": r["offset"], "action": act}
            for r, act in zip(recs, ["accept", "accept", "accept", "release", "reject"])]
    _, res = call("POST", f"{HTTP}/v1/share/{g}/ack", {"member": "A", "acks": acks})
    check("share ack", res["results"] == [True] * 5)
    _, b = call("POST", f"{HTTP}/v1/share/{g}/poll", {"member": "B", "topics": [t], "max_records": 50, "lock_ms": 60000})
    vals = sorted(r["value"] for r in b["records"])
    released = recs[3]["value"]
    check("released record redelivered to another member", released in vals and len(vals) == 6, f"{len(vals)} records")
    redelivered = [r for r in b["records"] if r["value"] == released]
    check("delivery count incremented", bool(redelivered) and redelivered[0]["delivery_count"] == 2)
    time.sleep(0.5)
    dead = []
    for p in range(3):
        s, d = call("GET", f"{HTTP}/v1/topics/{dlq}/partitions/{p}/records?offset=earliest")
        if s == 200:
            dead += d["records"]
    check("rejected record in DLQ with headers",
          len(dead) == 1 and dead[0]["headers"].get("bigpipe.dlq.reason") == "rejected by consumer",
          str([x["value"] for x in dead]))


def test_flow():
    src, dst = uid("pay"), uid("pay-big")
    call("POST", f"{ADMIN}/v1/topics", {"name": src, "partitions": 2})
    flow = uid("flow")
    s, r = call("POST", f"{ADMIN}/v1/flows", {
        "name": flow, "input": src, "output": dst,
        "processors": [{"filter": "this.amount > 1000"},
                       {"mapping": "root = this\nroot.card = deleted()\nroot.tier = 'big'"}]})
    check("deploy flow", s == 201, str(r))
    call("POST", f"{HTTP}/v1/topics/{src}/records", {"records": [
        {"value": {"amount": a, "card": "4111111111111111"}} for a in [10, 5000, 20, 7000, 9999]]})
    deadline = time.time() + 15
    out = []
    while time.time() < deadline and len(out) < 3:
        out = []
        for p in range(3):
            s, rr = call("GET", f"{HTTP}/v1/topics/{dst}/partitions/{p}/records?offset=earliest")
            if s == 200:
                out += rr["records"]
        time.sleep(0.3)
    vals = [json.loads(x["value"]) for x in out]
    ok = len(vals) == 3 and all("card" not in v and v["tier"] == "big" for v in vals)
    check("flow filters and maps records", ok, str(vals))
    _, flows = call("GET", f"{ADMIN}/v1/flows")
    st = [f for f in flows if f["name"] == flow][0]["stats"]
    check("flow stats", st["records_in"] == 5 and st["records_out"] == 3 and st["records_filtered"] == 2, str(st))
    call("DELETE", f"{ADMIN}/v1/flows/{flow}")


def test_rebalance():
    from confluent_kafka import Consumer
    t = uid("rb")
    call("POST", f"{ADMIN}/v1/topics", {"name": t, "partitions": 4})
    produce_kafka(t, 400)
    g = uid("rbg")
    counts = [0, 0]
    assigned = [set(), set()]
    stop = threading.Event()

    def run(i):
        c = Consumer({"bootstrap.servers": BOOT, "group.id": g, "auto.offset.reset": "earliest",
                      "session.timeout.ms": 6000, "heartbeat.interval.ms": 500})
        c.subscribe([t], on_assign=lambda cons, parts: assigned[i].update(p.partition for p in parts))
        while not stop.is_set():
            for m in c.consume(100, 0.5):
                if not m.error():
                    counts[i] += 1
        c.close()

    ths = [threading.Thread(target=run, args=(i,)) for i in range(2)]
    ths[0].start()
    time.sleep(1.5)
    ths[1].start()
    deadline = time.time() + 30
    while time.time() < deadline and sum(counts) < 400:
        time.sleep(0.3)
    time.sleep(2)
    stop.set()
    for th in ths:
        th.join()
    _, grp = call("GET", f"{ADMIN}/v1/groups/{g}")
    check("two consumers split partitions", len(assigned[1]) > 0 and sum(counts) >= 400, f"counts={counts} assigned={assigned}")
    check("group lag is zero after consumption", bool(grp) and grp["lag"] == 0, f"lag={grp and grp['lag']}")


def test_sse_and_http_groups():
    t = uid("sse")
    call("POST", f"{ADMIN}/v1/topics", {"name": t, "partitions": 1})
    events = []

    def listen():
        req = urllib.request.Request(f"{HTTP}/v1/topics/{t}/stream?from=earliest&filter=this.hot%20%3D%3D%20true")
        with urllib.request.urlopen(req, timeout=20) as r:
            for line in r:
                line = line.decode().strip()
                if line.startswith("data:") and '"offset"' in line:
                    events.append(json.loads(line[5:]))
                    if len(events) >= 2:
                        return

    th = threading.Thread(target=listen, daemon=True)
    th.start()
    time.sleep(0.5)
    call("POST", f"{HTTP}/v1/topics/{t}/records", [{"value": {"hot": True}}, {"value": {"hot": False}}, {"value": {"hot": True}}])
    th.join(15)
    check("SSE stream with server-side filter", len(events) == 2, f"{len(events)} events")

    g = uid("httpg")
    _, m1 = call("POST", f"{HTTP}/v1/groups/{g}/members", {"topics": [t]})
    _, r1 = call("GET", f"{HTTP}/v1/groups/{g}/members/{m1['member_id']}/records?timeout_ms=2000")
    check("HTTP group poll", len(r1["records"]) == 3, f"{len(r1['records'])}")
    _, r2 = call("GET", f"{HTTP}/v1/groups/{g}/members/{m1['member_id']}/records?timeout_ms=300")
    check("HTTP group auto-commit advances", len(r2["records"]) == 0)
    call("DELETE", f"{HTTP}/v1/groups/{g}/members/{m1['member_id']}")
    _, grp = call("GET", f"{ADMIN}/v1/groups/{g}")
    check("HTTP group committed offsets visible", grp["offsets"][0]["committed"] == 3 and grp["lag"] == 0)


if __name__ == "__main__":
    for fn in [test_diskless, test_migration, test_tiered, test_share_groups, test_flow, test_rebalance, test_sse_and_http_groups]:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            check(fn.__name__, False, repr(e))
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)

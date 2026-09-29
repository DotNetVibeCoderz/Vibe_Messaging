"""Integration tests: run against a live node (BIGPIPE_HTTP / BIGPIPE_ADMIN, default localhost)."""
import asyncio
import uuid

import pytest

from bigpipe import Admin, Consumer, Producer, ShareConsumer, stream


def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def admin():
    async with Admin() as a:
        yield a


async def test_produce_and_group_consume(admin):
    topic = uid("py-orders")
    await admin.create_topic(topic, partitions=3)
    async with Producer(linger_ms=10) as p:
        metas = await asyncio.gather(*(p.send(topic, {"id": i, "amount": i * 1000}, key=f"order-{i}",
                                               headers={"source": "pytest"}) for i in range(200)))
        assert len({m.partition for m in metas}) > 1
        raw = await p.send(topic, b"\x00\x01binary", key=b"\xff")
    seen = []
    async with Consumer(group=uid("g"), topics=[topic], auto_offset_reset="earliest") as c:
        async for msg in c:
            seen.append(msg)
            if len(seen) == 201:
                break
        await c.commit(seen[-1])
    ids = sorted(m.json()["id"] for m in seen if m.value and m.value.startswith(b"{"))
    assert ids == list(range(200))
    assert any(m.value == b"\x00\x01binary" and m.key == b"\xff" for m in seen)
    assert all(m.headers.get("source") == "pytest" for m in seen if m.value != b"\x00\x01binary")
    assert raw.offset >= 0


async def test_share_consumer_and_dlq(admin):
    topic = uid("py-jobs")
    dlq = topic + ".dlq"
    await admin.create_topic(topic, partitions=2)
    async with Producer() as p:
        await p.send_many(topic, [f"job-{i}" for i in range(5)])
    async with ShareConsumer(group=uid("w"), topics=[topic], max_attempts=2, dlq_topic=dlq) as w:
        done = set()
        rejected = False
        async for job in w:
            if job.value_str == "job-2":
                job.reject()
                rejected = True
            else:
                job.accept()
                done.add(job.value_str)
            if len(done) == 4 and rejected:
                break
        await w.flush_acks()
    for _ in range(20):
        try:
            dead = await admin.browse(dlq)
            if dead:
                break
        except Exception:  # noqa: BLE001 - topic appears asynchronously
            pass
        await asyncio.sleep(0.1)
    assert [d.value_str for d in dead] == ["job-2"]
    assert dead[0].headers["bigpipe.dlq.reason"] == "rejected by consumer"


async def test_sse_stream_with_filter(admin):
    topic = uid("py-sse")
    await admin.create_topic(topic, partitions=1)
    got = []

    async def reader():
        async for r in stream(topic, from_="earliest", filter="this.hot == true"):
            got.append(r.json())
            if len(got) == 2:
                return

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.3)
    async with Producer() as p:
        for v in [{"hot": True, "n": 1}, {"hot": False, "n": 2}, {"hot": True, "n": 3}]:
            await p.send(topic, v)
    await asyncio.wait_for(task, 15)
    assert [g["n"] for g in got] == [1, 3]


async def test_admin_migration_and_flow(admin):
    src = uid("py-src")
    await admin.create_topic(src, partitions=1)
    r = await admin.migrate(src, "diskless")
    assert r["to"] == "diskless"
    flow = uid("py-flow")
    await admin.deploy_flow(f"""
apiVersion: bigpipe.io/v1
kind: Flow
metadata:
  name: {flow}
spec:
  input:
    bigpipe:
      topic: {src}
  pipeline:
    processors:
      - filter: 'this.v > 1'
  output:
    bigpipe:
      topic: {src}-out
""")
    assert any(f["name"] == flow for f in await admin.flows())
    await admin.delete_flow(flow)

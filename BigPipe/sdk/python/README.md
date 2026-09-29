# bigpipe — Python SDK for BigPipe

Async Python client for [BigPipe](https://github.com/DotNetVibeCoderz/Vibe_Messaging/tree/main/BigPipe), the high performance realtime stream processing platform.

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*

```bash
pip install bigpipe
```

## Produce and consume

```python
import asyncio
from bigpipe import Producer, Consumer

async def main():
    async with Producer(bootstrap="localhost:9092") as p:
        meta = await p.send("orders", {"id": 1, "amount": 150000}, key="order-1", headers={"source": "checkout"})
        print(f"written to {meta.partition}@{meta.offset}")

    async with Consumer(bootstrap="localhost:9092", group="analytics", topics=["orders"],
                        auto_offset_reset="earliest") as c:
        async for msg in c:
            print(msg.key_str, msg.json())
            await c.commit(msg)

asyncio.run(main())
```

`Producer` batches everything sent within `linger_ms` into one request, so
`await asyncio.gather(*(p.send(...) for ...))` is fast. Values may be `bytes`, `str`
or anything JSON-serializable.

## Share groups (work queues with a dead-letter topic)

```python
from bigpipe import ShareConsumer

async with ShareConsumer(group="email-workers", topics=["email-jobs"],
                         max_attempts=5, dlq_topic="email-jobs.dlq") as worker:
    async for job in worker:
        try:
            await send_email(job.json())
            job.accept()
        except TemporaryError:
            job.release()   # redelivered to any worker
        except Exception:
            job.reject()    # goes to the dead-letter topic
```

## Streaming with a server-side filter

```python
from bigpipe import stream

async for r in stream("payments", from_="latest", filter='header("region") == "ID" && this.amount > 1000000'):
    print(r.json())
```

## Admin

```python
from bigpipe import Admin

async with Admin("http://localhost:9644") as admin:
    await admin.create_topic("clicks", partitions=12, mode="diskless")
    await admin.migrate("orders", "tiered")          # online, offsets unchanged
    print(await admin.group("analytics"))           # lag per partition
```

The SDK talks to the BigPipe HTTP gateway (default port 8082) and admin API (9644).
`bootstrap="host:9092"` maps to `http://host:8082`; pass `url=` to override.
Environment variables: `BIGPIPE_HTTP`, `BIGPIPE_ADMIN`, `BIGPIPE_API_KEY`.

License: Apache-2.0.

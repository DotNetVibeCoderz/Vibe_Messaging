# bigpipe-client — TypeScript / Node.js SDK for BigPipe

Zero-dependency client for [BigPipe](https://github.com/DotNetVibeCoderz/Vibe_Messaging/tree/main/BigPipe), the high performance realtime stream processing platform. Uses the built-in `fetch`, so it runs on Node.js 18+, Deno, Bun and edge runtimes.

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*

```bash
npm install bigpipe-client
```

## Produce and consume

```ts
import { Producer, Consumer } from "bigpipe-client";

const producer = new Producer({ bootstrap: ["localhost:9092"], compression: "zstd" });
const meta = await producer.send({ topic: "orders", key: "order-1", value: { id: 1, amount: 150000 } });
console.log(`written to ${meta.partition}@${meta.offset}`);
await producer.close();

const consumer = new Consumer({ bootstrap: ["localhost:9092"], group: "notifier", topics: ["orders"], autoOffsetReset: "earliest" });
for await (const msg of consumer) {
  console.log(msg.keyString, msg.json());
  await consumer.commit(msg);
}
```

Records sent within `lingerMs` share one request — `Promise.all` over many `send` calls is fast.

## Share groups

```ts
import { ShareConsumer } from "bigpipe-client";

const worker = new ShareConsumer({ group: "email-workers", topics: ["email-jobs"], maxAttempts: 5, deadLetterTopic: "email-jobs.dlq" });
for await (const job of worker) {
  try { await sendEmail(job.json()); job.accept(); }
  catch (e) { e.transient ? job.release() : job.reject(); }
}
```

## Streaming with a server-side filter

```ts
import { stream } from "bigpipe-client";

for await (const r of stream("payments", { filter: 'header("region") == "ID" && this.amount > 1000000' })) {
  console.log(r.json());
}
```

## Admin

```ts
import { Admin } from "bigpipe-client";

const admin = new Admin("http://localhost:9644");
await admin.createTopic("clicks", { partitions: 12, mode: "diskless" });
await admin.migrate("orders", "tiered"); // online; offsets never change
console.log(await admin.group("notifier"));
```

The SDK talks to the BigPipe HTTP gateway (port 8082) and admin API (port 9644).
`bootstrap: ["host:9092"]` maps to `http://host:8082`; pass `url` to override.
Environment variables: `BIGPIPE_HTTP`, `BIGPIPE_ADMIN`, `BIGPIPE_API_KEY`.

License: Apache-2.0.

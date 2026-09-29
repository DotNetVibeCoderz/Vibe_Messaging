// Integration tests against a live BigPipe node (BIGPIPE_HTTP / BIGPIPE_ADMIN, default localhost).
import assert from "node:assert/strict";
import { test } from "node:test";
import { Admin, Consumer, Producer, ShareConsumer, stream } from "../dist/index.js";

const uid = (p) => `${p}-${Math.random().toString(36).slice(2, 10)}`;
const admin = new Admin();

test("produce with batching and consume as a group", async () => {
  const topic = uid("ts-orders");
  await admin.createTopic(topic, { partitions: 3 });
  const producer = new Producer({ lingerMs: 10 });
  const metas = await Promise.all(
    Array.from({ length: 150 }, (_, i) => producer.send({ topic, key: `order-${i}`, value: { id: i }, headers: { source: "node" } })),
  );
  assert.ok(new Set(metas.map((m) => m.partition)).size > 1);
  await producer.send({ topic, value: new Uint8Array([0, 1, 2]) });
  await producer.close();

  const consumer = new Consumer({ group: uid("g"), topics: [topic], autoOffsetReset: "earliest" });
  const seen = [];
  for await (const msg of consumer) {
    seen.push(msg);
    if (seen.length === 151) break;
  }
  await consumer.commit(seen.at(-1));
  await consumer.close();
  const ids = seen.filter((m) => m.value[0] === 123).map((m) => m.json().id).sort((a, b) => a - b);
  assert.deepEqual(ids, Array.from({ length: 150 }, (_, i) => i));
  assert.ok(seen.some((m) => m.value.length === 3 && m.value[2] === 2));
});

test("share consumer acks and dead-letters", async () => {
  const topic = uid("ts-jobs");
  const dlq = `${topic}.dlq`;
  await admin.createTopic(topic, { partitions: 2 });
  const p = new Producer();
  await Promise.all(["a", "b", "c", "d"].map((v) => p.send({ topic, value: `job-${v}` })));
  await p.close();
  const worker = new ShareConsumer({ group: uid("w"), topics: [topic], maxAttempts: 2, deadLetterTopic: dlq });
  const done = new Set();
  let rejected = false;
  for await (const job of worker) {
    if (job.valueString === "job-c") {
      job.reject();
      rejected = true;
    } else {
      job.accept();
      done.add(job.valueString);
    }
    if (done.size === 3 && rejected) break;
  }
  await worker.close();
  let dead = [];
  for (let i = 0; i < 20 && dead.length === 0; i++) {
    dead = await admin.browse(dlq).catch(() => []);
    await new Promise((r) => setTimeout(r, 100));
  }
  assert.deepEqual(dead.map((d) => d.valueString), ["job-c"]);
});

test("SSE stream applies server-side filter", async () => {
  const topic = uid("ts-sse");
  await admin.createTopic(topic, { partitions: 1 });
  const got = [];
  const reading = (async () => {
    for await (const r of stream(topic, { from: "earliest", filter: "this.hot == true" })) {
      got.push(r.json().n);
      if (got.length === 2) return;
    }
  })();
  await new Promise((r) => setTimeout(r, 300));
  const p = new Producer({ lingerMs: 0 });
  for (const v of [{ hot: true, n: 1 }, { hot: false, n: 2 }, { hot: true, n: 3 }]) await p.send({ topic, value: v });
  await p.close();
  await reading;
  assert.deepEqual(got, [1, 3]);
});

test("admin migration", async () => {
  const topic = uid("ts-mig");
  await admin.createTopic(topic, { partitions: 1 });
  const r = await admin.migrate(topic, "tiered");
  assert.equal(r.to, "tiered");
  const again = await admin.createTopic(topic, { existOk: true });
  assert.equal(again.mode, "tiered");
});

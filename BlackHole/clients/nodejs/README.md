# BlackHole Messaging — Node.js client

*Gravicode Studios, led by Kang Fadhil.*

Node.js client for the BlackHole binary protocol: **RPC**, **Pub/Sub**, **Streaming** and
**Batching** over TCP. Speaks the same wire format as the [.NET library](../../README.md), verified
against it by the interop suite.

Requires Node 18+. ESM, no dependencies.

Published on [npm](https://www.npmjs.com/package/@gravicode/blackhole-messaging).

## Install

```bash
npm install @gravicode/blackhole-messaging
```

## Transports

TCP everywhere, plus the platform's own local IPC — a named pipe on Windows, a Unix domain socket
elsewhere:

```js
const client = await connect({ host: '127.0.0.1', port: 5000 });   // TCP
const client = await connectIpc('blackhole-gateway');              // named pipe on Windows
const client = await connectIpc('/tmp/blackhole.sock');            // Unix socket elsewhere
```

`connectIpc` expands a bare name to the full Windows pipe path, so the same string works on both
sides regardless of platform. `client.transportKind` reports `tcp`, `pipe` or `unix`. Shared memory
is .NET-only; see [docs/transports.md](../../docs/transports.md).

Compare them yourself:

```bash
node example/benchmark.js
```

## Thirty seconds

```js
import { connect } from '@gravicode/blackhole-messaging';

const client = await connect({ host: '127.0.0.1', port: 5000 });

// RPC
console.log(await client.callText('upper', 'halo blackhole'));   // HALO BLACKHOLE

// Pub/Sub, with MQTT-style wildcards
await client.subscribe('sensor/+/temperature', (topic, payload) => {
  console.log(topic, payload.toString('utf8'));
});
await client.publish('sensor/tank-3/temperature', '28.4');

await client.close();
```

## RPC

```js
const result = await client.call('echo', Buffer.from('bytes'));
const text = await client.callText('upper', 'halo', { timeout: 5000 });
await client.notify('log', 'fire and forget');
```

Every call has a deadline — `callTimeout` defaults to 30 seconds. Failures reject with `RpcError`
rather than hanging:

```js
import { RpcError } from '@gravicode/blackhole-messaging';

try {
  await client.call('risky', payload, { timeout: 5000 });
} catch (error) {
  if (error instanceof RpcError) {
    // The handler failed, the method is unknown, the deadline passed,
    // or the connection dropped mid-call.
    console.error(error.method, error.message);
  }
}
```

Serve methods the peer may call on you — handlers may return a Buffer, a string, or a promise:

```js
client.register('device/status', () => 'ok: 4 sensors online');
client.register('device/read', async (request) => readSensor(request.text()));
```

## Pub/Sub

`+` matches one segment, `#` matches the remainder.

```js
await client.subscribe('sensor/+/temperature', onReading);   // per-filter handler
await client.subscribe('alarm/#', onAlarm);
client.on('publish', (topic, payload) => { ... });           // everything

await client.publish('sensor/tank-3/temperature', '28.4');
await client.unsubscribe('alarm/#');
```

## Streaming

```js
import { createReadStream } from 'node:fs';

const sent = await client.sendStream('firmware-2026', createReadStream('firmware.bin'), {
  descriptor: new StreamDescriptor('firmware.bin', size, 'application/octet-stream'),
  chunkSize: 16 * 1024,
  progress: (sent) => console.log(`${(sent / 1024).toFixed(0)} KiB`),
});

client.on('stream', (streamId, descriptor, data) => save(streamId, data));
```

Accepts a Buffer or any async iterable, including a `fs.ReadStream`. Chunks accumulate and are
written once per 64 KiB rather than once per chunk, so a small chunk size does not mean a small
write.

## Batching

```js
import { Message, MessageType } from '@gravicode/blackhole-messaging';

await client.sendBatch(
  Array.from({ length: 1000 }, (_, i) =>
    new Message(MessageType.Publish, `log/entry/${i}`, Buffer.from(`line ${i}`)),
  ),
);
```

One frame, one socket write. The envelope holds complete BlackHole frames, so the peer unpacks it
with the same decoder and each message routes individually.

## Events

```js
client.on('publish', (topic, payload) => {});
client.on('stream', (streamId, descriptor, data) => {});
client.on('message', (message) => {});          // every frame
client.on('close', (error) => {});              // once, when the connection ends
client.on('error', (error) => {});
```

## Wire your handlers before the read loop starts

`configure` runs after the client is built but **before** anything is delivered. A server that
pushes the instant it accepts would otherwise beat a handler registered after `connect` resolves:

```js
const client = await connect({
  host, port,
  configure: (c) => c.register('client/identify', (r) => `tank-3:${r.text()}`),
});
```

## Payload ownership

Unlike the Go and .NET clients, decoded payloads are **copied** out of the read buffer, so a Buffer
you receive stays valid indefinitely. Keep it, queue it, hold it — no copy needed.

## Connection

```js
await client.ping();          // round trip in milliseconds
client.statistics;            // messages and bytes, both directions
client.isClosed;
await client.close();
```

`ping` is timed with `process.hrtime.bigint()`, the highest-resolution clock Node offers, so a
sub-millisecond loopback round trip is still measured accurately.

## Testing

```bash
node --test                          # 34 tests
node --test test/protocol.test.js    # codec only, no .NET needed
```

The interop suite starts the real .NET server and asserts against it. See
[../README.md](../README.md).

## Example

```bash
dotnet run --project ../../tests/BlackHole.InteropServer -- --port 5000
node example/demo.js --port 5000
```

---

*Built by Gravicode Studios, led by Kang Fadhil.*

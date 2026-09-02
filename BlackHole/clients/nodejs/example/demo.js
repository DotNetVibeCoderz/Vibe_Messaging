/**
 * Exercises every BlackHole pattern from Node.js against a running server.
 *
 * BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
 *
 * Start a server first, for instance:
 *
 *   dotnet run --project tests/BlackHole.InteropServer -- --port 5000
 *
 * then:
 *
 *   node example/demo.js --port 5000
 */

import { parseArgs } from 'node:util';

import {
  BlackHoleClient,
  Message,
  MessageType,
  RpcError,
  StreamDescriptor,
} from '../src/index.js';

const { values } = parseArgs({
  options: {
    host: { type: 'string', default: '127.0.0.1' },
    port: { type: 'string', default: '5000' },
  },
});

const host = values.host;
const port = Number(values.port);

const client = await BlackHoleClient.connectWithRetry({
  host,
  port,
  attempts: 5,
  configure: (c) => {
    // Registered before the read loop delivers anything, so a server that calls back the instant
    // it accepts cannot beat this registration.
    c.register('client/identify', (request) => `node-example:${request.text()}`);
  },
});

console.log(`connected to ${host}:${port}`);

try {
  // --- RPC -------------------------------------------------------------------
  const shouted = await client.callText('upper', 'halo blackhole');
  console.log(`rpc        : upper("halo blackhole") -> "${shouted}"`);

  try {
    await client.call('does-not-exist', undefined, { timeout: 2000 });
  } catch (error) {
    if (error instanceof RpcError) console.log(`rpc error  : ${error.message}`);
    else throw error;
  }

  // --- Pub/Sub ---------------------------------------------------------------
  const delivered = new Promise((resolve) => {
    client.subscribe('sensor/+/temperature', (topic, payload) =>
      resolve(`${topic} = ${payload.toString('utf8')}`),
    );
  });
  await new Promise((resolve) => setTimeout(resolve, 300));

  await client.publish('sensor/tank-3/temperature', '28.4');
  await client.publish('sensor/tank-3/humidity', '62'); // matches no filter

  const timeout = new Promise((resolve) => setTimeout(() => resolve('nothing arrived'), 3000));
  console.log('pubsub     :', await Promise.race([delivered, timeout]));

  // --- Streaming -------------------------------------------------------------
  const payload = Buffer.from('blackhole'.repeat(128 * 1024)); // about 1.1 MiB
  let started = process.hrtime.bigint();
  const sent = await client.sendStream('example-upload', payload, {
    descriptor: new StreamDescriptor('example.bin', payload.length),
    chunkSize: 16 * 1024,
  });
  let elapsed = Number(process.hrtime.bigint() - started) / 1e6;
  console.log(`streaming  : ${(sent / (1024 * 1024)).toFixed(1)} MiB in ${elapsed.toFixed(0)} ms`);

  // --- Batching --------------------------------------------------------------
  const batch = Array.from(
    { length: 1000 },
    (_, i) => new Message(MessageType.Publish, 'log/entry', Buffer.from(`line ${i}`)),
  );
  started = process.hrtime.bigint();
  await client.sendBatch(batch);
  elapsed = Number(process.hrtime.bigint() - started) / 1e6;
  console.log(`batching   : ${batch.length} messages in one write, ${elapsed.toFixed(1)} ms`);

  // --- Server calling back into this client ----------------------------------
  try {
    const answer = await client.callText('callback', 'hello');
    console.log(`callback   : server asked, client answered "${answer}"`);
  } catch (error) {
    console.log(`callback   : ${error.message}`);
  }

  // --- Connection ------------------------------------------------------------
  console.log(`keepalive  : ${(await client.ping()).toFixed(3)} ms round trip`);
  console.log(
    `statistics : sent ${client.statistics.messagesSent} msg / ${client.statistics.bytesSent} B, ` +
      `received ${client.statistics.messagesReceived} msg / ${client.statistics.bytesReceived} B`,
  );
} finally {
  await client.close();
}

console.log();
console.log('Gravicode Studios - led by Kang Fadhil');

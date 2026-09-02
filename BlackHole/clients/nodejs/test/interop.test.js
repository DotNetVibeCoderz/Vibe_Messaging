/**
 * Interop against the real .NET server.
 *
 * BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
 *
 * Every test here talks to `tests/BlackHole.InteropServer`, which is the actual library. If the
 * JavaScript codec and the C# codec ever disagree by a single byte, these fail.
 */

import test, { after, before } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import readline from 'node:readline';

import { BlackHoleClient, Message, MessageType, RpcError, StreamDescriptor } from '../src/index.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '..', '..', '..');
const SERVER_PROJECT = path.join(REPO_ROOT, 'tests', 'BlackHole.InteropServer');

/** @type {import('node:child_process').ChildProcess | null} */
let server = null;
let port = 0;

before(async () => {
  const built = path.join(SERVER_PROJECT, 'bin', 'Release', 'net10.0', 'BlackHole.InteropServer.exe');

  const [command, args] = existsSync(built)
    ? [built, ['--port', '0']]
    : ['dotnet', ['run', '--project', SERVER_PROJECT, '-c', 'Release', '--', '--port', '0']];

  server = spawn(command, args, { cwd: REPO_ROOT, stdio: ['ignore', 'pipe', 'ignore'] });

  port = await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('the interop server did not report a port within 120s')),
      120_000,
    );

    const lines = readline.createInterface({ input: server.stdout });
    lines.on('line', (line) => {
      if (line.startsWith('READY ')) {
        clearTimeout(timer);
        lines.close();
        resolve(Number(line.slice('READY '.length).trim()));
      }
    });
    server.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`the interop server exited with code ${code} before it was ready`));
    });
  });
}, { timeout: 180_000 });

after(async () => {
  if (server) server.kill();
});

/** @param {Parameters<typeof BlackHoleClient.connect>[0]} [options] */
async function connect(options = {}) {
  return BlackHoleClient.connect({ host: '127.0.0.1', port, ...options });
}

/** Runs `body` with a connected client and always closes it. */
async function withClient(body, options) {
  const client = await connect(options);
  try {
    return await body(client);
  } finally {
    await client.close();
  }
}

// ------------------------------------------------------------------------ RPC

test('echo returns the exact bytes', async () => {
  await withClient(async (client) => {
    const payload = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
    assert.deepEqual(await client.call('echo', payload), payload);
  });
});

test('text round trip', async () => {
  await withClient(async (client) => {
    assert.equal(await client.callText('upper', 'halo blackhole'), 'HALO BLACKHOLE');
  });
});

test('non-ASCII survives the round trip', async () => {
  // echo, not upper: casing rules differ between runtimes, and what matters here is that the UTF-8
  // bytes cross unchanged in both directions.
  await withClient(async (client) => {
    const original = 'suhu tangki 28,4 °C — αβγ — 日本語 — 🕳';
    const result = await client.call('echo', Buffer.from(original, 'utf8'));
    assert.equal(result.toString('utf8'), original);
  });
});

test('numeric payload', async () => {
  await withClient(async (client) => {
    const result = await client.call('sum', Buffer.from([1, 2, 3, 4, 5]));
    assert.equal(result.readInt32LE(0), 15);
  });
});

test('many concurrent calls stay correlated', async () => {
  await withClient(async (client) => {
    const results = await Promise.all(
      Array.from({ length: 200 }, (_, i) => client.callText('upper', `call-${i}`)),
    );
    assert.deepEqual(
      results,
      Array.from({ length: 200 }, (_, i) => `CALL-${i}`),
    );
  });
});

test('a handler failure surfaces', async () => {
  await withClient(async (client) => {
    await assert.rejects(() => client.call('boom'), (error) => {
      assert.ok(error instanceof RpcError);
      assert.equal(error.method, 'boom');
      assert.match(error.message, /boom/);
      return true;
    });
  });
});

test('an unknown method fails fast', async () => {
  await withClient(async (client) => {
    const started = Date.now();
    await assert.rejects(() => client.call('no-such-method'), /Unknown method/);
    assert.ok(Date.now() - started < 5_000, 'an unknown method should fail immediately');
  });
});

test('a deadline is enforced', async () => {
  await withClient(async (client) => {
    await assert.rejects(() => client.callText('sleep', '30000', { timeout: 300 }), /deadline/);
  });
});

test('a late reply does not break the next call', async () => {
  await withClient(async (client) => {
    await assert.rejects(() => client.callText('sleep', '400', { timeout: 150 }));
    await new Promise((resolve) => setTimeout(resolve, 600));
    assert.equal(await client.callText('upper', 'still here'), 'STILL HERE');
  });
});

test('large payloads cross intact', async () => {
  await withClient(async (client) => {
    for (const size of [1, 1024, 64 * 1024, 1024 * 1024]) {
      const result = await client.call('big', Buffer.from(String(size)));
      assert.equal(result.length, size, `size ${size}`);
      assert.deepEqual(
        result,
        Buffer.from(Array.from({ length: size }, (_, i) => i % 251)),
        `size ${size} content`,
      );
    }
  });
}, { timeout: 60_000 });

test('the server can call back into the client', async () => {
  await withClient(
    async (client) => {
      assert.equal(await client.callText('callback', 'hello'), 'node-sdk:hello');
    },
    {
      configure: (client) => {
        client.register('client/identify', (request) => `node-sdk:${request.text()}`);
      },
    },
  );
});

// -------------------------------------------------------------------- Pub/Sub

test('publish reaches a subscriber, and a non-matching topic does not', async () => {
  const subscriber = await connect();
  const publisher = await connect();

  try {
    /** @type {string[]} */
    const seen = [];
    const first = new Promise((resolve) => {
      subscriber.subscribe('sensor/+/temperature', (topic, payload) => {
        seen.push(`${topic}=${payload.toString('utf8')}`);
        resolve();
      });
    });
    await new Promise((resolve) => setTimeout(resolve, 300));

    await publisher.publish('sensor/tank-3/temperature', '28.4');
    await publisher.publish('sensor/tank-3/humidity', '62');
    await first;
    await new Promise((resolve) => setTimeout(resolve, 400));

    assert.deepEqual(seen, ['sensor/tank-3/temperature=28.4']);
  } finally {
    await subscriber.close();
    await publisher.close();
  }
});

test('multi-segment wildcard', async () => {
  const subscriber = await connect();
  const publisher = await connect();

  try {
    const seen = new Set();
    const both = new Promise((resolve) => {
      subscriber.subscribe('alarm/#', (topic) => {
        seen.add(topic);
        if (seen.size === 2) resolve();
      });
    });
    await new Promise((resolve) => setTimeout(resolve, 300));

    await publisher.publish('alarm/floor-1/pump', 'overheating');
    await publisher.publish('alarm/floor-2/valve/inlet', 'stuck');
    await both;

    assert.deepEqual([...seen].sort(), ['alarm/floor-1/pump', 'alarm/floor-2/valve/inlet']);
  } finally {
    await subscriber.close();
    await publisher.close();
  }
});

test('unsubscribe stops delivery', async () => {
  const subscriber = await connect();
  const publisher = await connect();

  try {
    let count = 0;
    await subscriber.subscribe('news', () => {
      count += 1;
    });
    await new Promise((resolve) => setTimeout(resolve, 300));

    await publisher.publish('news', 'one');
    await new Promise((resolve) => setTimeout(resolve, 400));

    await subscriber.unsubscribe('news');
    await new Promise((resolve) => setTimeout(resolve, 200));
    await publisher.publish('news', 'two');
    await new Promise((resolve) => setTimeout(resolve, 400));

    assert.equal(count, 1);
  } finally {
    await subscriber.close();
    await publisher.close();
  }
});

// ------------------------------------------------------------------ streaming

test('a stream arrives complete', async () => {
  await withClient(async (client) => {
    const payload = Buffer.from(Array.from({ length: 512 * 1024 }, (_, i) => (i * 7) % 256));

    const confirmed = new Promise((resolve) => {
      client.subscribe('stream/done', (_topic, body) => resolve(body.toString('utf8')));
    });
    await new Promise((resolve) => setTimeout(resolve, 300));

    const sent = await client.sendStream('firmware-2026', payload, {
      descriptor: new StreamDescriptor('firmware.bin', payload.length, 'application/octet-stream'),
      chunkSize: 16 * 1024,
    });

    assert.equal(sent, payload.length);
    assert.equal(await confirmed, `firmware-2026:${payload.length}`);
  });
}, { timeout: 60_000 });

test('progress is reported', async () => {
  await withClient(async (client) => {
    /** @type {number[]} */
    const reports = [];
    await client.sendStream('progress-check', Buffer.alloc(256 * 1024), {
      chunkSize: 4096,
      progress: (sent) => reports.push(sent),
    });

    assert.ok(reports.length > 0, 'expected at least one progress report');
    assert.equal(reports.at(-1), 256 * 1024);
  });
});

// ------------------------------------------------------------------- batching

test('batched messages are routed individually', async () => {
  const subscriber = await connect();
  const publisher = await connect();

  try {
    const count = 300;
    let seen = 0;
    const done = new Promise((resolve) => {
      subscriber.subscribe('log/#', () => {
        if ((seen += 1) === count) resolve();
      });
    });
    await new Promise((resolve) => setTimeout(resolve, 300));

    await publisher.sendBatch(
      Array.from(
        { length: count },
        (_, i) => new Message(MessageType.Publish, `log/entry/${i}`, Buffer.from(`line ${i}`)),
      ),
    );
    await done;

    assert.equal(seen, count);
  } finally {
    await subscriber.close();
    await publisher.close();
  }
}, { timeout: 30_000 });

// ----------------------------------------------------------------- connection

test('the keepalive round trip is measured', async () => {
  await withClient(async (client) => {
    const elapsed = await client.ping();
    assert.ok(elapsed > 0 && elapsed < 5_000, `implausible round trip: ${elapsed}ms`);
    assert.equal(client.statistics.lastRoundTrip, elapsed);
  });
});

test('statistics count both directions', async () => {
  await withClient(async (client) => {
    for (let i = 0; i < 25; i++) await client.callText('upper', 'abc');

    assert.ok(client.statistics.messagesSent >= 25);
    assert.ok(client.statistics.messagesReceived >= 25);
    assert.ok(client.statistics.bytesSent > 0);
  });
});

test('pending calls fail when the connection closes', async () => {
  const client = await connect();
  const pending = client.callText('sleep', '30000', { timeout: 20_000 });

  await new Promise((resolve) => setTimeout(resolve, 300));
  await client.close();

  await assert.rejects(() => pending, RpcError);
});

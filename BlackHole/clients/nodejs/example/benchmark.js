/**
 * Compares transports from the Node.js client.
 *
 * BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
 *
 * Starts the .NET interop server once per transport and measures the same workload over each, so
 * the numbers differ only in how the bytes travel. Run it with:
 *
 *   node example/benchmark.js
 *
 * Needs a .NET 10 SDK, or a prebuilt tests/BlackHole.InteropServer.
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { fileURLToPath } from 'node:url';

import { BlackHoleClient, Message, MessageType, connectIpc } from '../src/index.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '..', '..', '..');
const SERVER_PROJECT = path.join(REPO_ROOT, 'tests', 'BlackHole.InteropServer');

const CALLS = 5_000;
const WARMUP = 500;
const BATCH_MESSAGES = 50_000;

/** Starts the interop server on one transport and resolves once it reports READY. */
function startServer(args) {
  const built = path.join(SERVER_PROJECT, 'bin', 'Release', 'net10.0', 'BlackHole.InteropServer.exe');
  const [command, argv] = existsSync(built)
    ? [built, args]
    : ['dotnet', ['run', '--project', SERVER_PROJECT, '-c', 'Release', '--', ...args]];

  const child = spawn(command, argv, { cwd: REPO_ROOT, stdio: ['ignore', 'pipe', 'ignore'] });

  const ready = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('server did not report READY within 120s')), 120_000);
    const lines = readline.createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      if (line.startsWith('READY ')) {
        clearTimeout(timer);
        lines.close();
        resolve(line.slice('READY '.length).trim());
      }
    });
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`server exited with code ${code} before it was ready`));
    });
  });

  return { child, ready };
}

/** Sequential RPC round trips, reported as percentiles. */
async function measureLatency(client) {
  for (let i = 0; i < WARMUP; i++) await client.callText('upper', 'x');

  const samples = new Array(CALLS);
  for (let i = 0; i < CALLS; i++) {
    const started = process.hrtime.bigint();
    await client.callText('upper', 'x');
    samples[i] = Number(process.hrtime.bigint() - started) / 1000; // microseconds
  }
  samples.sort((a, b) => a - b);

  const at = (q) => samples[Math.min(samples.length - 1, Math.ceil(q * samples.length) - 1)];
  return { p50: at(0.5), p90: at(0.9), p99: at(0.99) };
}

/** Publishes, one at a time and then batched, to show what batching is worth on this transport. */
async function measureThroughput(client) {
  const payload = Buffer.from('28.4');

  let started = process.hrtime.bigint();
  for (let i = 0; i < BATCH_MESSAGES; i++) await client.publish('t', payload);
  const individual = Number(process.hrtime.bigint() - started) / 1e9;

  const batch = Array.from(
    { length: 256 },
    () => new Message(MessageType.Publish, 't', payload),
  );
  started = process.hrtime.bigint();
  for (let sent = 0; sent < BATCH_MESSAGES; sent += batch.length) await client.sendBatch(batch);
  const batched = Number(process.hrtime.bigint() - started) / 1e9;

  return { individual: BATCH_MESSAGES / individual, batched: BATCH_MESSAGES / batched };
}

async function run(label, serverArgs, connect) {
  const { child, ready } = startServer(serverArgs);
  try {
    const endpoint = await ready;
    const client = await connect(endpoint);
    try {
      const latency = await measureLatency(client);
      const throughput = await measureThroughput(client);
      console.log(
        `  ${label.padEnd(14)} ${latency.p50.toFixed(1).padStart(8)}us ` +
          `${latency.p90.toFixed(1).padStart(8)}us ${latency.p99.toFixed(1).padStart(8)}us   ` +
          `${Math.round(throughput.individual).toLocaleString('en-US').padStart(11)}/s ` +
          `${Math.round(throughput.batched).toLocaleString('en-US').padStart(11)}/s`,
      );
    } finally {
      await client.close();
    }
  } finally {
    child.kill();
  }
}

console.log('==========================================================');
console.log('  BLACKHOLE MESSAGING - NODE.JS TRANSPORT COMPARISON');
console.log('==========================================================');
console.log(`  node         : ${process.version}`);
console.log(`  platform     : ${process.platform} ${process.arch}`);
console.log(`  measured     : ${new Date().toISOString().slice(0, 16).replace('T', ' ')}`);
console.log(`  workload     : ${CALLS.toLocaleString('en-US')} RPC calls, ` +
  `${BATCH_MESSAGES.toLocaleString('en-US')} publishes`);
console.log();
console.log('  transport            p50       p90       p99      one-by-one     batched(256)');
console.log('  --------------   --------  --------  --------   -------------  -------------');

await run('TCP loopback', ['--port', '0'], (endpoint) =>
  BlackHoleClient.connect({ host: '127.0.0.1', port: Number(endpoint) }),
);

// A named pipe on Windows, a Unix domain socket elsewhere - one option, the platform's own IPC.
const ipcName = `bh-bench-${process.pid}`;
if (process.platform === 'win32') {
  await run('Named pipe', ['--pipe', ipcName], () => connectIpc(ipcName, { dialTimeout: 20_000 }));
} else {
  const socketPath = path.join('/tmp', `${ipcName}.sock`);
  await run('Unix socket', ['--unix', socketPath], () => connectIpc(socketPath, { dialTimeout: 20_000 }));
}

console.log();
console.log('  Shared memory is .NET-only: it needs a mapped segment and a dedicated polling');
console.log('  thread, which is not something these SDKs can offer honestly. See docs/transports.md.');
console.log();
console.log('Gravicode Studios - led by Kang Fadhil');

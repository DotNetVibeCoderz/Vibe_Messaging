/**
 * Node.js client for BlackHole Messaging.
 *
 * BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
 *
 * @module
 */

import net from 'node:net';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';

import {
  DEFAULT_MAX_FRAME_LENGTH,
  Message,
  MessageFlags,
  MessageType,
  RpcError,
  StreamDescriptor,
  decodeFrame,
  encodeFrame,
  topicMatches,
} from './protocol.js';

/**
 * A connected BlackHole client.
 *
 * Emits:
 * - `publish` — `(topic, payload)` for every delivered message
 * - `stream`  — `(streamId, descriptor, data)` for each completed inbound stream
 * - `message` — `(message)` for every frame, after the built-in handling
 * - `close`   — `(error|null)` once, when the connection ends
 * - `error`   — `(error)` when the read loop or a handler fails
 *
 * @extends EventEmitter
 */
export class BlackHoleClient extends EventEmitter {
  /**
   * @param {net.Socket} socket
   * @param {{ maxFrameLength?: number, callTimeout?: number, flushThreshold?: number }} [options]
   */
  constructor(socket, options = {}) {
    super();

    this._socket = socket;
    this._maxFrameLength = options.maxFrameLength ?? DEFAULT_MAX_FRAME_LENGTH;
    this._flushThreshold = options.flushThreshold ?? 64 * 1024;

    /** Milliseconds a call waits before rejecting with {@link RpcError}. */
    this.callTimeout = options.callTimeout ?? 30_000;

    /** Live counters for this connection. */
    this.statistics = {
      messagesSent: 0,
      messagesReceived: 0,
      bytesSent: 0,
      bytesReceived: 0,
      /** @type {number|null} Milliseconds for the most recent keepalive round trip. */
      lastRoundTrip: null,
    };

    this._buffer = Buffer.alloc(0);
    this._pending = new Map();
    this._correlation = 0;
    this._methods = new Map();
    this._subscriptions = [];
    this._streams = new Map();
    /** @type {Array<(elapsed: number) => void>} */
    this._pongWaiters = [];
    this._closed = false;
    this._closeError = null;

    socket.on('data', (chunk) => this._onData(chunk));
    socket.on('error', (error) => this._close(error));
    socket.on('close', () => this._close(null));
  }

  // ------------------------------------------------------------------ setup

  /**
   * Dial a server and start receiving.
   *
   * `configure` runs after the client is built but **before** the read loop delivers anything, so
   * handlers registered there cannot miss a message a server pushes the instant it accepts.
   * Registering after this resolves is a race for that first message.
   *
   * @param {{ host?: string, port: number, dialTimeout?: number, maxFrameLength?: number,
   *           callTimeout?: number, flushThreshold?: number,
   *           configure?: (client: BlackHoleClient) => void }} options
   * @returns {Promise<BlackHoleClient>}
   */
  static connect(options) {
    const { host = '127.0.0.1', port, dialTimeout = 10_000, configure, ...rest } = options;

    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host, port });
      // BlackHole coalesces at the application layer, so letting the kernel hold small frames only
      // adds latency.
      socket.setNoDelay(true);

      const timer = setTimeout(() => {
        socket.destroy();
        reject(new Error(`blackhole: connecting to ${host}:${port} timed out after ${dialTimeout}ms`));
      }, dialTimeout);

      socket.once('error', (error) => {
        clearTimeout(timer);
        reject(error);
      });

      socket.once('connect', () => {
        clearTimeout(timer);
        socket.removeAllListeners('error');
        const client = new BlackHoleClient(socket, rest);
        if (configure) configure(client);
        resolve(client);
      });
    });
  }

  /**
   * Dial with exponential backoff, for a client that may start before its server.
   * @param {Parameters<typeof BlackHoleClient.connect>[0] & { attempts?: number, initialDelay?: number }} options
   * @returns {Promise<BlackHoleClient>}
   */
  static async connectWithRetry(options) {
    const { attempts = 5, initialDelay = 100, ...rest } = options;
    let delay = initialDelay;
    let last;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        return await BlackHoleClient.connect(rest);
      } catch (error) {
        last = error;
        if (attempt === attempts) break;
        await new Promise((resolve) => setTimeout(resolve, delay));
        delay = Math.min(delay * 2, 5_000);
      }
    }
    throw new Error(`blackhole: could not connect after ${attempts} attempts: ${last?.message}`);
  }

  // ------------------------------------------------------------------- send

  /**
   * Write one message to the peer.
   * @param {Message} message
   * @returns {Promise<void>}
   */
  send(message) {
    return this._write(encodeFrame(message), 1);
  }

  /**
   * Write several messages in one socket write.
   *
   * Unlike {@link sendBatch} the peer sees each message individually framed; this only saves
   * syscalls on the sending side.
   * @param {Message[]} messages
   * @returns {Promise<void>}
   */
  sendMany(messages) {
    if (!messages.length) return Promise.resolve();
    return this._write(Buffer.concat(messages.map(encodeFrame)), messages.length);
  }

  /**
   * @param {Buffer} frames
   * @param {number} count
   * @returns {Promise<void>}
   */
  _write(frames, count) {
    if (this._closed) {
      return Promise.reject(new Error('blackhole: the connection is closed'));
    }

    return new Promise((resolve, reject) => {
      this._socket.write(frames, (error) => {
        if (error) {
          reject(error);
          return;
        }
        this.statistics.messagesSent += count;
        this.statistics.bytesSent += frames.length;
        resolve();
      });
    });
  }

  // -------------------------------------------------------------------- RPC

  /**
   * Call a remote method and wait for its reply.
   *
   * Rejects with {@link RpcError} when the method fails, is unknown, times out, or the connection
   * drops before the reply arrives.
   *
   * @param {string} method
   * @param {Buffer|Uint8Array|string} [payload]
   * @param {{ timeout?: number }} [options]
   * @returns {Promise<Buffer>}
   */
  async call(method, payload = Buffer.alloc(0), options = {}) {
    const body = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : Buffer.from(payload);
    const correlationId = ++this._correlation;
    const timeout = options.timeout ?? this.callTimeout;

    const reply = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(correlationId);
        reject(new RpcError(method, `Call to '${method}' did not complete before its deadline.`));
      }, timeout);

      this._pending.set(correlationId, { method, resolve, reject, timer });
    });

    try {
      await this.send(new Message(MessageType.RpcRequest, method, body, correlationId));
    } catch (error) {
      const pending = this._pending.get(correlationId);
      if (pending) {
        clearTimeout(pending.timer);
        this._pending.delete(correlationId);
      }
      throw error;
    }

    return reply;
  }

  /**
   * Text-in, text-out convenience wrapper around {@link call}.
   * @param {string} method
   * @param {string} [payload]
   * @param {{ timeout?: number }} [options]
   * @returns {Promise<string>}
   */
  async callText(method, payload = '', options = {}) {
    const result = await this.call(method, Buffer.from(payload, 'utf8'), options);
    return result.toString('utf8');
  }

  /**
   * Fire and forget: send a request and never wait for a reply.
   * @param {string} method
   * @param {Buffer|Uint8Array|string} [payload]
   */
  notify(method, payload = Buffer.alloc(0)) {
    const body = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : Buffer.from(payload);
    return this.send(new Message(MessageType.RpcRequest, method, body, 0, MessageFlags.NoReply));
  }

  /**
   * Serve a method the peer may call on this client.
   *
   * The handler receives the request {@link Message} and returns a Buffer, a string, or a promise
   * of either. Throwing sends the peer an error response.
   *
   * @param {string} method
   * @param {(request: Message) => (Buffer|string|Promise<Buffer|string>)} handler
   * @returns {this}
   */
  register(method, handler) {
    this._methods.set(method, handler);
    return this;
  }

  // ---------------------------------------------------------------- Pub/Sub

  /**
   * Ask the broker for a topic or wildcard filter.
   *
   * `+` matches one segment, `#` matches the remainder. When `handler` is given it fires only for
   * topics matching this filter; listen to the `publish` event for everything.
   *
   * @param {string} filter
   * @param {(topic: string, payload: Buffer) => void} [handler]
   */
  async subscribe(filter, handler) {
    if (handler) this._subscriptions.push({ filter, handler });
    await this.send(new Message(MessageType.Subscribe, filter));
  }

  /**
   * Stop receiving a filter.
   * @param {string} filter
   */
  async unsubscribe(filter) {
    this._subscriptions = this._subscriptions.filter((s) => s.filter !== filter);
    await this.send(new Message(MessageType.Unsubscribe, filter));
  }

  /**
   * Publish to a topic.
   * @param {string} topic
   * @param {Buffer|Uint8Array|string} payload
   */
  publish(topic, payload) {
    const body = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : Buffer.from(payload);
    return this.send(new Message(MessageType.Publish, topic, body));
  }

  // -------------------------------------------------------------- streaming

  /**
   * Send a large body as chunks and resolve with the bytes sent.
   *
   * Chunks are accumulated and written once per `flushThreshold` bytes rather than once per chunk,
   * which is what keeps small chunk sizes fast.
   *
   * @param {string} streamId
   * @param {Buffer|Uint8Array|Readable} data
   * @param {{ descriptor?: StreamDescriptor, chunkSize?: number,
   *           progress?: (sent: number) => void }} [options]
   * @returns {Promise<number>}
   */
  async sendStream(streamId, data, options = {}) {
    const chunkSize = options.chunkSize ?? 16 * 1024;
    const isBuffer = Buffer.isBuffer(data) || data instanceof Uint8Array;
    const total = isBuffer ? data.length : StreamDescriptor.UNKNOWN_LENGTH;

    const descriptor =
      options.descriptor ?? new StreamDescriptor(streamId, total, 'application/octet-stream');

    await this.send(new Message(MessageType.StreamStart, streamId, descriptor.encode()));

    let sent = 0;
    let index = 0;
    /** @type {Buffer[]} */
    let pending = [];
    let pendingBytes = 0;

    const flush = async () => {
      if (!pending.length) return;
      const frames = Buffer.concat(pending);
      pending = [];
      pendingBytes = 0;
      await this._write(frames, 1);
      if (options.progress) options.progress(sent);
    };

    try {
      const source = isBuffer ? chunkBuffer(Buffer.from(data), chunkSize) : data;

      for await (const chunk of source) {
        const body = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        for (let start = 0; start < body.length; start += chunkSize) {
          const slice = body.subarray(start, Math.min(start + chunkSize, body.length));
          pending.push(encodeFrame(new Message(MessageType.StreamChunk, streamId, slice, index++)));
          pendingBytes += slice.length;
          sent += slice.length;
          if (pendingBytes >= this._flushThreshold) await flush();
        }
      }

      await flush();
      await this.send(new Message(MessageType.StreamEnd, streamId, Buffer.alloc(0), index));
      if (options.progress) options.progress(sent);
      return sent;
    } catch (error) {
      try {
        await this.send(
          new Message(
            MessageType.StreamAbort,
            streamId,
            Buffer.from(String(error?.message ?? error), 'utf8'),
            0,
            MessageFlags.Error,
          ),
        );
      } catch {
        // The connection is already gone; the original failure is what matters.
      }
      throw error;
    }
  }

  // --------------------------------------------------------------- batching

  /**
   * Pack several messages into one frame and one socket write.
   *
   * The envelope payload is a run of complete BlackHole frames, which is exactly what the peer's
   * own codec unpacks - there is no second wire format.
   *
   * @param {Message[]} messages
   */
  sendBatch(messages) {
    if (!messages.length) return Promise.resolve();
    const payload = Buffer.concat(messages.map(encodeFrame));
    return this.send(new Message(MessageType.Batch, '', payload, messages.length));
  }

  // -------------------------------------------------------------- read loop

  /** @param {Buffer} chunk */
  _onData(chunk) {
    this._buffer = this._buffer.length ? Buffer.concat([this._buffer, chunk]) : chunk;

    let offset = 0;
    try {
      for (;;) {
        const parsed = decodeFrame(this._buffer, offset, this._maxFrameLength);
        if (!parsed) break;
        offset += parsed.consumed;
        this.statistics.messagesReceived += 1;
        this.statistics.bytesReceived += parsed.consumed;
        this._dispatch(parsed.message);
      }
    } catch (error) {
      this._close(error);
      return;
    } finally {
      // Keep only the partial frame at the end.
      this._buffer = offset ? this._buffer.subarray(offset) : this._buffer;
    }
  }

  /** @param {Message} message */
  _dispatch(message) {
    switch (message.type) {
      case MessageType.Ping:
        // Answered here so keepalive never reaches application code.
        this.send(new Message(MessageType.Pong, '', Buffer.alloc(0), message.correlationId)).catch(
          () => {},
        );
        return;

      case MessageType.Pong: {
        const waiters = this._pongWaiters;
        this._pongWaiters = [];
        for (const resolve of waiters) resolve();
        return;
      }

      case MessageType.RpcResponse:
        this._completeCall(message);
        return;

      case MessageType.RpcRequest:
        this._serveCall(message);
        return;

      case MessageType.Batch:
        this._unpackBatch(message);
        return;

      case MessageType.StreamStart:
      case MessageType.StreamChunk:
      case MessageType.StreamEnd:
      case MessageType.StreamAbort:
        this._handleStream(message);
        break;

      case MessageType.Publish:
        this._deliverPublish(message);
        break;

      default:
        break;
    }

    this.emit('message', message);
  }

  /** @param {Message} message */
  _completeCall(message) {
    const pending = this._pending.get(message.correlationId);
    if (!pending) return; // Late reply for a call that already timed out.

    this._pending.delete(message.correlationId);
    clearTimeout(pending.timer);

    if (message.isError) {
      pending.reject(new RpcError(pending.method, message.text()));
    } else {
      pending.resolve(message.payload);
    }
  }

  /** @param {Message} message */
  async _serveCall(message) {
    const handler = this._methods.get(message.header);

    if (!handler) {
      await this.send(
        new Message(
          MessageType.RpcResponse,
          message.header,
          Buffer.from(`Unknown method '${message.header}'.`, 'utf8'),
          message.correlationId,
          MessageFlags.Error,
        ),
      ).catch(() => {});
      return;
    }

    try {
      const result = await handler(message);
      if (message.flags & MessageFlags.NoReply) return;

      const payload =
        result == null
          ? Buffer.alloc(0)
          : typeof result === 'string'
            ? Buffer.from(result, 'utf8')
            : Buffer.from(result);

      await this.send(
        new Message(MessageType.RpcResponse, message.header, payload, message.correlationId),
      );
    } catch (error) {
      await this.send(
        new Message(
          MessageType.RpcResponse,
          message.header,
          Buffer.from(`${error?.name ?? 'Error'}: ${error?.message ?? error}`, 'utf8'),
          message.correlationId,
          MessageFlags.Error,
        ),
      ).catch(() => {});
    }
  }

  /** @param {Message} message */
  _deliverPublish(message) {
    for (const { filter, handler } of this._subscriptions) {
      if (topicMatches(filter, message.header)) {
        try {
          handler(message.header, message.payload);
        } catch (error) {
          this.emit('error', error);
        }
      }
    }
    this.emit('publish', message.header, message.payload);
  }

  /** @param {Message} message */
  _unpackBatch(message) {
    let offset = 0;
    for (;;) {
      const parsed = decodeFrame(message.payload, offset, this._maxFrameLength);
      if (!parsed) return;
      offset += parsed.consumed;
      // One level only: a nested envelope is a loop waiting to happen.
      if (parsed.message.type !== MessageType.Batch) this._dispatch(parsed.message);
    }
  }

  /** @param {Message} message */
  _handleStream(message) {
    switch (message.type) {
      case MessageType.StreamStart:
        this._streams.set(message.header, {
          descriptor: StreamDescriptor.decode(message.payload),
          chunks: [],
          received: 0,
          nextChunk: 0,
        });
        return;

      case MessageType.StreamChunk: {
        const state = this._streams.get(message.header);
        if (!state) return;
        if (message.correlationId !== state.nextChunk) {
          this._streams.delete(message.header); // Out of order: abandon rather than corrupt.
          return;
        }
        state.nextChunk += 1;
        state.chunks.push(message.payload);
        state.received += message.payload.length;
        return;
      }

      case MessageType.StreamEnd: {
        const state = this._streams.get(message.header);
        if (!state) return;
        this._streams.delete(message.header);
        this.emit('stream', message.header, state.descriptor, Buffer.concat(state.chunks));
        return;
      }

      case MessageType.StreamAbort:
        this._streams.delete(message.header);
        return;

      default:
        return;
    }
  }

  // ------------------------------------------------------------- lifecycle

  /**
   * Send a keepalive probe and resolve with the round trip in milliseconds.
   * @param {number} [timeout]
   * @returns {Promise<number>}
   */
  async ping(timeout = 5_000) {
    const started = process.hrtime.bigint();

    const answered = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pongWaiters = this._pongWaiters.filter((w) => w !== waiter);
        reject(new Error('blackhole: the peer did not answer the keepalive probe'));
      }, timeout);

      const waiter = () => {
        clearTimeout(timer);
        resolve();
      };
      this._pongWaiters.push(waiter);
    });

    await this.send(new Message(MessageType.Ping));
    await answered;

    const elapsed = Number(process.hrtime.bigint() - started) / 1e6;
    this.statistics.lastRoundTrip = elapsed;
    return elapsed;
  }

  /** True once the connection has ended. */
  get isClosed() {
    return this._closed;
  }

  /** Close the connection and fail every pending call. */
  async close() {
    this._close(null);
    await new Promise((resolve) => {
      if (this._socket.destroyed) {
        resolve();
        return;
      }
      this._socket.end(() => resolve());
      this._socket.destroy();
    });
  }

  /** @param {Error|null} error */
  _close(error) {
    if (this._closed) return;
    this._closed = true;
    this._closeError = error;

    const reason = error?.message ?? 'The connection closed before the reply arrived.';
    for (const [, pending] of this._pending) {
      clearTimeout(pending.timer);
      pending.reject(new RpcError(pending.method, reason));
    }
    this._pending.clear();

    for (const resolve of this._pongWaiters) resolve();
    this._pongWaiters = [];

    this.emit('close', error);
    // An 'error' with no listener would take the process down, so it is only raised when someone
    // is listening.
    if (error && this.listenerCount('error') > 0) this.emit('error', error);
  }
}

/**
 * Yield a buffer in slices, so a Buffer source and a stream source share one loop.
 * @param {Buffer} buffer
 * @param {number} size
 */
function* chunkBuffer(buffer, size) {
  for (let offset = 0; offset < buffer.length; offset += size) {
    yield buffer.subarray(offset, Math.min(offset + size, buffer.length));
  }
}

/**
 * Shorthand for {@link BlackHoleClient.connect}.
 * @param {Parameters<typeof BlackHoleClient.connect>[0]} options
 */
export function connect(options) {
  return BlackHoleClient.connect(options);
}

// SocketSignal - Node.js client
// Built by Gravicode Studios, led by Kang Fadhil.
//
// No dependencies: Node 22 ships a global WebSocket, which is all this protocol needs.

/** The remote handler threw. */
export class SignalInvocationError extends Error {
  constructor(method, remoteMessage) {
    super(`Remote method '${method}' failed: ${remoteMessage}`);
    this.name = "SignalInvocationError";
    this.method = method;
    this.remoteMessage = remoteMessage;
  }
}

/** The reply did not arrive in time. */
export class SignalTimeoutError extends Error {
  constructor(method, ms) {
    super(`Remote method '${method}' did not answer within ${ms} ms.`);
    this.name = "SignalTimeoutError";
    this.method = method;
  }
}

/** The socket went away with calls still in flight. */
export class SignalClosedError extends Error {
  constructor(reason) {
    super(`The connection closed before the call completed: ${reason}`);
    this.name = "SignalClosedError";
  }
}

/**
 * A SocketSignal client.
 *
 * @example
 * const client = new SocketSignalClient();
 * client.on("serverHello", (text) => `node heard ${text}`);
 * await client.connect("ws://localhost:8080/ws/");
 * console.log(await client.call("sum", 5, 7));
 */
export class SocketSignalClient extends EventTarget {
  #socket = null;
  #handlers = new Map();
  #pending = new Map();
  #nextId = 0;
  #url = null;
  #keepAlive = null;
  #closed = false;

  /**
   * @param {object} [options]
   * @param {number} [options.callTimeoutMs=30000] How long `call` waits for a reply.
   * @param {number} [options.keepAliveMs=15000]   Ping interval; 0 disables.
   * @param {boolean} [options.autoReconnect=false]
   * @param {number} [options.reconnectDelayMs=1000] First backoff step; it doubles to 30 s.
   */
  constructor(options = {}) {
    super();
    this.callTimeoutMs = options.callTimeoutMs ?? 30_000;
    this.keepAliveMs = options.keepAliveMs ?? 15_000;
    this.autoReconnect = options.autoReconnect ?? false;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1_000;
    this.clientId = null;
  }

  get connected() {
    return this.#socket?.readyState === WebSocket.OPEN;
  }

  /**
   * Registers a method the server may call. The return value becomes the reply when the server
   * asked for one; throwing sends the error back instead.
   *
   * @param {string} method
   * @param {(...args: any[]) => any} handler
   */
  on(method, handler) {
    this.#handlers.set(method, handler);
    return this;
  }

  /** Removes a registration. */
  off(method) {
    return this.#handlers.delete(method);
  }

  /** Dials the server and resolves once the welcome frame lands. */
  connect(url) {
    this.#url = url ?? this.#url;
    this.#closed = false;

    return new Promise((resolve, reject) => {
      const socket = new WebSocket(this.#url);
      this.#socket = socket;

      const settled = { done: false };
      const fail = (error) => {
        if (settled.done) return;
        settled.done = true;
        reject(error);
      };

      socket.addEventListener("message", (event) => {
        const frame = this.#parse(event.data);
        if (!frame) return;

        if (frame.type === "welcome") {
          this.clientId = frame.id;
          this.#startKeepAlive();
          this.dispatchEvent(new CustomEvent("connected", { detail: frame.id }));
          if (!settled.done) {
            settled.done = true;
            resolve(frame.id);
          }
          return;
        }
        this.#dispatch(frame);
      });

      socket.addEventListener("error", () => fail(new SignalClosedError("the socket could not be opened")));

      socket.addEventListener("close", (event) => {
        this.#stopKeepAlive();
        const reason = event.reason || `code ${event.code}`;
        for (const [, call] of this.#pending) call.reject(new SignalClosedError(reason));
        this.#pending.clear();
        this.dispatchEvent(new CustomEvent("disconnected", { detail: reason }));
        fail(new SignalClosedError(reason));
        if (this.autoReconnect && !this.#closed) this.#reconnect();
      });
    });
  }

  /** Calls a server method and waits for its return value. */
  call(method, ...args) {
    if (!this.connected) return Promise.reject(new SignalClosedError("the client is not connected"));

    const id = String(++this.#nextId);
    const frame = { type: "invoke", id, method, args, expectReturn: true };

    return new Promise((resolve, reject) => {
      const timer = this.callTimeoutMs > 0
        ? setTimeout(() => {
            this.#pending.delete(id);
            reject(new SignalTimeoutError(method, this.callTimeoutMs));
          }, this.callTimeoutMs)
        : null;

      this.#pending.set(id, {
        method,
        resolve: (value) => { if (timer) clearTimeout(timer); resolve(value); },
        reject: (error) => { if (timer) clearTimeout(timer); reject(error); },
      });

      this.#socket.send(JSON.stringify(frame));
    });
  }

  /** Calls a server method without waiting for a reply. */
  send(method, ...args) {
    if (!this.connected) throw new SignalClosedError("the client is not connected");
    this.#socket.send(JSON.stringify({
      type: "invoke", id: String(++this.#nextId), method, args, expectReturn: false,
    }));
  }

  /** Closes the connection and stops reconnecting. */
  close() {
    this.#closed = true;
    this.autoReconnect = false;
    this.#stopKeepAlive();
    this.#socket?.close();
  }

  // -------------------------------------------------------------------------------------

  #parse(data) {
    try {
      return JSON.parse(typeof data === "string" ? data : data.toString("utf8"));
    } catch {
      return null;
    }
  }

  async #dispatch(frame) {
    switch (frame.type) {
      case "invoke": {
        const handler = this.#handlers.get(frame.method);
        if (!handler) {
          if (frame.expectReturn) this.#reply(frame.id, undefined, `Method '${frame.method}' not found`);
          return;
        }
        try {
          const result = await handler(...(frame.args ?? []));
          if (frame.expectReturn) this.#reply(frame.id, result ?? null, null);
        } catch (error) {
          if (frame.expectReturn) this.#reply(frame.id, undefined, String(error?.message ?? error));
        }
        return;
      }

      case "result": {
        const call = this.#pending.get(frame.id);
        if (!call) return;
        this.#pending.delete(frame.id);
        if (frame.error) call.reject(new SignalInvocationError(call.method, frame.error));
        else call.resolve(frame.result ?? null);
        return;
      }

      case "ping":
        this.#socket.send(JSON.stringify({ type: "pong", id: frame.id }));
        return;

      default:
        // pong, and anything a newer server invents
        return;
    }
  }

  #reply(id, result, error) {
    if (!this.connected) return;
    this.#socket.send(error === null || error === undefined
      ? JSON.stringify({ type: "result", id, result })
      : JSON.stringify({ type: "result", id, error }));
  }

  #startKeepAlive() {
    if (this.keepAliveMs <= 0) return;
    this.#stopKeepAlive();
    this.#keepAlive = setInterval(() => {
      if (this.connected) this.#socket.send(JSON.stringify({ type: "ping", id: String(++this.#nextId) }));
    }, this.keepAliveMs);
    this.#keepAlive.unref?.();
  }

  #stopKeepAlive() {
    if (this.#keepAlive) clearInterval(this.#keepAlive);
    this.#keepAlive = null;
  }

  async #reconnect() {
    let delay = this.reconnectDelayMs;
    for (let attempt = 1; !this.#closed; attempt++) {
      this.dispatchEvent(new CustomEvent("reconnecting", { detail: attempt }));
      await new Promise((r) => setTimeout(r, delay));
      try {
        await this.connect();
        return;
      } catch {
        delay = Math.min(delay * 2, 30_000);
      }
    }
  }
}

export default SocketSignalClient;

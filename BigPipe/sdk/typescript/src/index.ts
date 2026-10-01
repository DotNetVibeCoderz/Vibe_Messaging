/**
 * BigPipe TypeScript / Node.js SDK.
 *
 * Talks to the BigPipe HTTP gateway (port 8082) and admin API (port 9644) with the built-in
 * `fetch`, so it has no runtime dependencies and also runs in Deno, Bun and edge runtimes.
 *
 * Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
 */

export type Value = Uint8Array | string | number | boolean | null | object;

/** A consumed record. `key`/`value` are bytes; use `valueString` / `json()` for text. */
export class BigPipeRecord {
  constructor(
    public readonly topic: string,
    public readonly partition: number,
    public readonly offset: number,
    public readonly timestamp: number,
    public readonly key: Uint8Array | null,
    public readonly value: Uint8Array | null,
    public readonly headers: Record<string, string | null>,
    public readonly deliveryCount?: number,
  ) {}

  get keyString(): string | null {
    return this.key === null ? null : new TextDecoder().decode(this.key);
  }

  get valueString(): string | null {
    return this.value === null ? null : new TextDecoder().decode(this.value);
  }

  /** Parses the value as JSON. */
  json<T = unknown>(): T {
    return JSON.parse(this.valueString ?? "null") as T;
  }

  /** @internal */
  static fromJson(d: any): BigPipeRecord {
    return new BigPipeRecord(
      d.topic,
      d.partition,
      d.offset,
      d.timestamp,
      decode(d.key, d.key_encoding),
      decode(d.value, d.value_encoding),
      d.headers ?? {},
      d.delivery_count ?? undefined,
    );
  }
}

export interface RecordMetadata {
  topic: string;
  partition: number;
  offset: number;
}

export class BigPipeError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string) {
    super(`${status} ${code}: ${message}`);
    this.name = "BigPipeError";
  }
}

function decode(v: string | null | undefined, encoding?: string): Uint8Array | null {
  if (v === null || v === undefined) return null;
  return encoding === "base64" ? new Uint8Array(Buffer.from(v, "base64")) : new TextEncoder().encode(v);
}

function env(name: string): string | undefined {
  return typeof process !== "undefined" ? process.env?.[name] : undefined;
}

/** Resolves the gateway URL: explicit url, or `host` of a Kafka bootstrap list on port 8082. */
function gatewayUrl(url?: string, bootstrap?: string | string[]): string {
  if (url) return url.replace(/\/$/, "");
  const b = Array.isArray(bootstrap) ? bootstrap[0] : bootstrap?.split(",")[0];
  if (b) return `http://${b.trim().replace(/:\d+$/, "")}:8082`;
  return (env("BIGPIPE_HTTP") ?? "http://localhost:8082").replace(/\/$/, "");
}

async function call<T>(base: string, method: string, path: string, body?: unknown, init?: { apiKey?: string; raw?: string; contentType?: string; query?: Record<string, unknown> }): Promise<T> {
  const url = new URL(base + path);
  for (const [k, v] of Object.entries(init?.query ?? {})) if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  const headers: Record<string, string> = {};
  if (init?.apiKey) headers.authorization = `Bearer ${init.apiKey}`;
  let payload: string | undefined;
  if (init?.raw !== undefined) {
    payload = init.raw;
    headers["content-type"] = init.contentType ?? "text/plain";
  } else if (body !== undefined) {
    payload = JSON.stringify(body);
    headers["content-type"] = "application/json";
  }
  const resp = await fetch(url, { method, headers, body: payload });
  const text = await resp.text();
  if (!resp.ok) {
    let code = "error";
    let message = text;
    try {
      const e = JSON.parse(text).error;
      code = e?.code ?? code;
      message = e?.message ?? message;
    } catch {
      /* not JSON */
    }
    throw new BigPipeError(resp.status, code, message);
  }
  return (text ? JSON.parse(text) : undefined) as T;
}

function encodeValue(v: Value | undefined): Record<string, unknown> {
  if (v instanceof Uint8Array) return { value: Buffer.from(v).toString("base64"), value_encoding: "base64" };
  return { value: v ?? null };
}

function encodeKey(k: Uint8Array | string | undefined | null): Record<string, unknown> {
  if (k === undefined || k === null) return {};
  if (k instanceof Uint8Array) return { key: Buffer.from(k).toString("base64"), key_encoding: "base64" };
  return { key: String(k) };
}

// ------------------------------------------------------------------------------------------------
// Producer
// ------------------------------------------------------------------------------------------------

export interface ProducerOptions {
  url?: string;
  bootstrap?: string | string[];
  /** Accepted for parity with Kafka SDKs; the gateway acknowledges after the broker append. */
  acks?: "all" | "leader" | "none";
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  lingerMs?: number;
  maxBatch?: number;
}

export interface SendRecord {
  topic: string;
  value?: Value;
  key?: Uint8Array | string | null;
  headers?: Record<string, string>;
  partition?: number;
  timestamp?: number;
}

interface Pending {
  rec: Record<string, unknown>;
  resolve: (m: RecordMetadata) => void;
  reject: (e: unknown) => void;
}

/**
 * Batching producer: records sent within `lingerMs` to a topic share one request.
 *
 * ```ts
 * const producer = new Producer({ bootstrap: ["localhost:9092"] });
 * const meta = await producer.send({ topic: "orders", key: "order-1", value: { id: 1 } });
 * await producer.close();
 * ```
 */
export class Producer {
  private readonly base: string;
  private readonly pending = new Map<string, Pending[]>();
  private timer?: ReturnType<typeof setTimeout>;
  private readonly linger: number;
  private readonly maxBatch: number;
  private readonly inflight = new Set<Promise<void>>();

  constructor(private readonly options: ProducerOptions = {}) {
    this.base = gatewayUrl(options.url, options.bootstrap);
    this.linger = Math.max(0, options.lingerMs ?? 5);
    this.maxBatch = Math.max(1, options.maxBatch ?? 500);
  }

  /** Kept for API parity; the producer connects lazily. */
  async connect(): Promise<void> {}

  send(record: SendRecord): Promise<RecordMetadata> {
    const rec: Record<string, unknown> = { ...encodeKey(record.key), ...encodeValue(record.value) };
    if (record.headers) rec.headers = record.headers;
    if (record.partition !== undefined) rec.partition = record.partition;
    if (record.timestamp !== undefined) rec.timestamp = record.timestamp;
    return new Promise<RecordMetadata>((resolve, reject) => {
      const list = this.pending.get(record.topic) ?? [];
      list.push({ rec, resolve, reject });
      this.pending.set(record.topic, list);
      if (list.length >= this.maxBatch || this.linger === 0) this.flushSoon(0);
      else this.flushSoon(this.linger);
    });
  }

  private flushSoon(delay: number): void {
    if (this.timer && delay > 0) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush();
    }, delay);
  }

  /** Sends every queued record now. */
  async flush(): Promise<void> {
    const batches: [string, Pending[]][] = [];
    for (const [topic, items] of this.pending) {
      for (let i = 0; i < items.length; i += this.maxBatch) batches.push([topic, items.slice(i, i + this.maxBatch)]);
    }
    this.pending.clear();
    const work = Promise.all(batches.map(([t, b]) => this.post(t, b))).then(() => undefined);
    this.inflight.add(work);
    try {
      await work;
    } finally {
      this.inflight.delete(work);
    }
  }

  private async post(topic: string, batch: Pending[]): Promise<void> {
    try {
      const body: Record<string, unknown> = { records: batch.map((p) => p.rec) };
      if (this.options.compression && this.options.compression !== "none") body.compression = this.options.compression;
      const res = await call<{ offsets: { partition: number; offset: number }[] }>(this.base, "POST", `/v1/topics/${encodeURIComponent(topic)}/records`, body);
      batch.forEach((p, i) => p.resolve({ topic, partition: res.offsets[i].partition, offset: res.offsets[i].offset }));
    } catch (e) {
      batch.forEach((p) => p.reject(e));
    }
  }

  async close(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    await this.flush();
    await Promise.all(this.inflight);
  }
}

// ------------------------------------------------------------------------------------------------
// Consumer (server-assigned group)
// ------------------------------------------------------------------------------------------------

export interface ConsumerOptions {
  url?: string;
  bootstrap?: string | string[];
  group: string;
  topics: string[];
  autoOffsetReset?: "earliest" | "latest";
  autoCommit?: boolean;
  /** bpql filter evaluated by the broker. */
  filter?: string;
  pollTimeoutMs?: number;
  maxRecords?: number;
}

/**
 * Consumer-group member; BigPipe assigns partitions server-side.
 *
 * ```ts
 * const consumer = new Consumer({ bootstrap: ["localhost:9092"], group: "notifier", topics: ["orders"] });
 * for await (const msg of consumer) { console.log(msg.json()); await consumer.commit(msg); }
 * ```
 */
export class Consumer implements AsyncIterable<BigPipeRecord> {
  private readonly base: string;
  memberId?: string;
  assignment: { topic: string; partition: number }[] = [];
  private closed = false;

  constructor(private readonly options: ConsumerOptions) {
    if (!options.topics?.length) throw new Error("subscribe to at least one topic");
    this.base = gatewayUrl(options.url, options.bootstrap);
  }

  async connect(): Promise<void> {
    const res = await call<{ member_id: string }>(this.base, "POST", `/v1/groups/${encodeURIComponent(this.options.group)}/members`, {
      topics: this.options.topics,
      auto_commit: this.options.autoCommit ?? true,
      offset_reset: this.options.autoOffsetReset ?? "latest",
      filter: this.options.filter,
    });
    this.memberId = res.member_id;
  }

  /** Next batch of records (empty after the poll timeout). */
  async poll(timeoutMs?: number): Promise<BigPipeRecord[]> {
    if (!this.memberId) await this.connect();
    try {
      const res = await call<{ records: any[]; assignment: { topic: string; partition: number }[] }>(
        this.base,
        "GET",
        `/v1/groups/${encodeURIComponent(this.options.group)}/members/${this.memberId}/records`,
        undefined,
        { query: { timeout_ms: timeoutMs ?? this.options.pollTimeoutMs ?? 1000, max_records: this.options.maxRecords ?? 500 } },
      );
      this.assignment = res.assignment ?? [];
      return res.records.map(BigPipeRecord.fromJson);
    } catch (e) {
      if (e instanceof BigPipeError && e.status === 404) {
        await this.connect(); // session expired: rejoin
        return [];
      }
      throw e;
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<BigPipeRecord> {
    while (!this.closed) {
      for (const r of await this.poll()) yield r;
    }
  }

  /** Commits the position after `record` (or every position returned so far). */
  async commit(record?: BigPipeRecord): Promise<void> {
    const body = record ? { offsets: [{ topic: record.topic, partition: record.partition, offset: record.offset + 1 }] } : {};
    await call(this.base, "POST", `/v1/groups/${encodeURIComponent(this.options.group)}/members/${this.memberId}/commit`, body);
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.memberId) {
      await call(this.base, "DELETE", `/v1/groups/${encodeURIComponent(this.options.group)}/members/${this.memberId}`).catch(() => undefined);
      this.memberId = undefined;
    }
  }
}

// ------------------------------------------------------------------------------------------------
// Share groups
// ------------------------------------------------------------------------------------------------

export interface ShareConsumerOptions {
  url?: string;
  bootstrap?: string | string[];
  group: string;
  topics: string[];
  member?: string;
  maxRecords?: number;
  lockMs?: number;
  maxAttempts?: number;
  deadLetterTopic?: string;
  filter?: string;
  pollTimeoutMs?: number;
}

export type AckAction = "accept" | "release" | "reject";

export class ShareRecord extends BigPipeRecord {
  /** @internal */
  owner!: ShareConsumer;

  accept(): void {
    this.owner.ack(this, "accept");
  }

  release(): void {
    this.owner.ack(this, "release");
  }

  reject(): void {
    this.owner.ack(this, "reject");
  }
}

/** Queue semantics: records are locked to one worker and acknowledged individually. */
export class ShareConsumer implements AsyncIterable<ShareRecord> {
  private readonly base: string;
  readonly member: string;
  private acks: { topic: string; partition: number; offset: number; action: AckAction }[] = [];
  private closed = false;

  constructor(private readonly options: ShareConsumerOptions) {
    this.base = gatewayUrl(options.url, options.bootstrap);
    this.member = options.member ?? `ts-${Math.random().toString(36).slice(2, 12)}`;
  }

  /** @internal */
  ack(r: BigPipeRecord, action: AckAction): void {
    this.acks.push({ topic: r.topic, partition: r.partition, offset: r.offset, action });
  }

  async flushAcks(): Promise<boolean[]> {
    if (!this.acks.length) return [];
    const acks = this.acks;
    this.acks = [];
    const res = await call<{ results: boolean[] }>(this.base, "POST", `/v1/share/${encodeURIComponent(this.options.group)}/ack`, { member: this.member, acks });
    return res.results;
  }

  async poll(): Promise<ShareRecord[]> {
    await this.flushAcks();
    const res = await call<{ records: any[] }>(this.base, "POST", `/v1/share/${encodeURIComponent(this.options.group)}/poll`, {
      member: this.member,
      topics: this.options.topics,
      max_records: this.options.maxRecords ?? 100,
      lock_ms: this.options.lockMs ?? 30000,
      max_attempts: this.options.maxAttempts ?? 5,
      dlq_topic: this.options.deadLetterTopic,
      filter: this.options.filter,
      timeout_ms: this.options.pollTimeoutMs ?? 1000,
    });
    return res.records.map((d) => {
      const b = BigPipeRecord.fromJson(d);
      const r = new ShareRecord(b.topic, b.partition, b.offset, b.timestamp, b.key, b.value, b.headers, b.deliveryCount);
      r.owner = this;
      return r;
    });
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ShareRecord> {
    while (!this.closed) {
      for (const r of await this.poll()) yield r;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.flushAcks();
  }
}

// ------------------------------------------------------------------------------------------------
// Server-Sent Events
// ------------------------------------------------------------------------------------------------

export interface StreamOptions {
  url?: string;
  bootstrap?: string | string[];
  from?: "earliest" | "latest" | number;
  /** bpql filter evaluated by the broker, e.g. `header("region") == "ID" && this.amount > 1000`. */
  filter?: string;
  group?: string;
  signal?: AbortSignal;
}

/** Streams a topic over SSE. Break out of the loop (or abort the signal) to stop. */
export async function* stream(topic: string, options: StreamOptions = {}): AsyncGenerator<BigPipeRecord> {
  const url = new URL(`${gatewayUrl(options.url, options.bootstrap)}/v1/topics/${encodeURIComponent(topic)}/stream`);
  url.searchParams.set("from", String(options.from ?? "latest"));
  if (options.filter) url.searchParams.set("filter", options.filter);
  if (options.group) url.searchParams.set("group", options.group);
  const controller = new AbortController();
  options.signal?.addEventListener("abort", () => controller.abort());
  const resp = await fetch(url, { headers: { accept: "text/event-stream" }, signal: controller.signal });
  if (!resp.ok || !resp.body) {
    const text = await resp.text();
    throw new BigPipeError(resp.status, "stream_failed", text);
  }
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let event: string | undefined;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, nl).replace(/\r$/, "");
        buffer = buffer.slice(nl + 1);
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:") && event === "record") yield BigPipeRecord.fromJson(JSON.parse(line.slice(5)));
        else if (line === "") event = undefined;
      }
    }
  } finally {
    controller.abort();
  }
}

// ------------------------------------------------------------------------------------------------
// Admin
// ------------------------------------------------------------------------------------------------

export type StorageMode = "local" | "tiered" | "diskless";

export class Admin {
  private readonly base: string;
  private readonly apiKey?: string;

  constructor(url?: string, options: { apiKey?: string } = {}) {
    this.base = (url ?? env("BIGPIPE_ADMIN") ?? "http://localhost:9644").replace(/\/$/, "");
    this.apiKey = options.apiKey ?? env("BIGPIPE_API_KEY");
  }

  private req<T>(method: string, path: string, body?: unknown, query?: Record<string, unknown>): Promise<T> {
    return call<T>(this.base, method, path, body, { apiKey: this.apiKey, query });
  }

  cluster(): Promise<any> {
    return this.req("GET", "/v1/cluster");
  }

  metrics(): Promise<any> {
    return this.req("GET", "/v1/metrics");
  }

  topics(): Promise<any[]> {
    return this.req("GET", "/v1/topics");
  }

  topic(name: string): Promise<any> {
    return this.req("GET", `/v1/topics/${encodeURIComponent(name)}`);
  }

  async createTopic(name: string, options: { partitions?: number; mode?: StorageMode; config?: Record<string, string>; existOk?: boolean } = {}): Promise<any> {
    try {
      return await this.req("POST", "/v1/topics", { name, partitions: options.partitions ?? 3, mode: options.mode ?? "local", config: options.config ?? {} });
    } catch (e) {
      if (options.existOk && e instanceof BigPipeError && e.status === 409) return this.topic(name);
      throw e;
    }
  }

  deleteTopic(name: string): Promise<void> {
    return this.req("DELETE", `/v1/topics/${encodeURIComponent(name)}`);
  }

  updateConfig(name: string, changes: Record<string, string | null>): Promise<any> {
    return this.req("PATCH", `/v1/topics/${encodeURIComponent(name)}/config`, { config: changes });
  }

  /** Online storage-mode migration: new data uses `to`, offsets never change. */
  migrate(name: string, to: StorageMode): Promise<any> {
    return this.req("POST", `/v1/topics/${encodeURIComponent(name)}/migrate`, { to });
  }

  /** Compacts a `cleanup.policy=compact` topic now (keeps the latest record per key). */
  compact(name: string): Promise<any> {
    return this.req("POST", `/v1/topics/${encodeURIComponent(name)}/compact`, {});
  }

  async browse(topic: string, options: { limit?: number; partition?: number; offset?: string; filter?: string } = {}): Promise<BigPipeRecord[]> {
    const res = await this.req<{ records: any[] }>("GET", `/v1/topics/${encodeURIComponent(topic)}/messages`, undefined, {
      limit: options.limit ?? 50,
      partition: options.partition,
      offset: options.offset,
      filter: options.filter,
    });
    return res.records.map(BigPipeRecord.fromJson);
  }

  groups(): Promise<any[]> {
    return this.req("GET", "/v1/groups");
  }

  group(id: string): Promise<any> {
    return this.req("GET", `/v1/groups/${encodeURIComponent(id)}`);
  }

  resetGroup(id: string, topic: string, to: "earliest" | "latest" | "offset", offset?: number): Promise<any> {
    return this.req("POST", `/v1/groups/${encodeURIComponent(id)}/reset`, { topic, to, offset });
  }

  flows(): Promise<any[]> {
    return this.req("GET", "/v1/flows");
  }

  /** Deploys a flow from a JSON spec object or a YAML document string. */
  deployFlow(spec: object | string): Promise<any> {
    if (typeof spec === "string") return call(this.base, "POST", "/v1/flows", undefined, { apiKey: this.apiKey, raw: spec, contentType: "application/yaml" });
    return this.req("POST", "/v1/flows", spec);
  }

  deleteFlow(name: string): Promise<void> {
    return this.req("DELETE", `/v1/flows/${encodeURIComponent(name)}`);
  }
}

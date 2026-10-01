# HTTP and admin API reference

[English](http-api.md) · [Bahasa Indonesia](../id/http-api.md)

BigPipe exposes three HTTP surfaces:

| Port | Surface | Used by |
|---|---|---|
| 8082 | **Data gateway**: produce, fetch, SSE, HTTP consumer groups, share groups | Python/TS/Go/Java SDKs, browsers, `curl` |
| 9644 | **Admin API**: topics, groups, flows, migration, message browser | Console, `bpctl`, `BigPipeAdminClient` |
| 9650 | **AdminApi gateway** (.NET, optional): the admin API with API keys, roles and an audit log | operators and automation |

All bodies are JSON. Errors look like `{"error":{"code":"not_found","message":"..."}}` and use the matching HTTP status.

## Record format

Every record returned by the gateway looks like this:

```json
{"topic":"orders","partition":1,"offset":0,"timestamp":1790682099229,
 "key":"k1","key_encoding":"utf8","value":"{\"amount\":5}","value_encoding":"utf8",
 "headers":{"region":"ID"}}
```

Keys and values that are valid UTF-8 are returned as strings (`"utf8"`). Anything else is base64 (`"base64"`).

---

## Data gateway (8082)

### Produce: `POST /v1/topics/{topic}/records`

```json
{"records":[
  {"key":"order-1","value":{"id":1,"amount":150000},"headers":{"region":"ID"}},
  {"value":"aGVsbG8=","value_encoding":"base64","partition":2,"timestamp":1790000000000}
 ],
 "compression":"zstd"}
```

Response: `{"topic":"orders","offsets":[{"partition":1,"offset":0}, ...]}`.

- A JSON `value` (object, array, number) is stored as compact JSON text. A string is stored as-is.
- Without `partition`, keyed records use Kafka's murmur2 partitioner (the same partition a Java client would pick). Records without a key are spread across partitions.
- `compression`: `none` (default), `gzip`, `snappy`, `lz4` or `zstd`.
- A bare array (`[{...},{...}]`) is also accepted as the body.

### Read a partition: `GET /v1/topics/{topic}/partitions/{p}/records`

| Query | Default | Meaning |
|---|---|---|
| `offset` | `earliest` | `earliest`, `latest` or a number |
| `limit` | 500 | maximum records |
| `max_bytes` | 1 MiB | maximum payload bytes |
| `timeout_ms` | 0 | long-poll until data arrives |
| `filter` | — | bpql expression evaluated by the broker |

Response: `{"records":[...],"next_offset":1,"high_watermark":1}`.

### Stream: `GET /v1/topics/{topic}/stream` (Server-Sent Events)

| Query | Meaning |
|---|---|
| `from` | `latest` (default), `earliest` or an offset |
| `partitions` | comma-separated list, e.g. `0,2` |
| `filter` | bpql expression; only matching records are sent |
| `group` | join an HTTP consumer group; partitions are assigned and offsets committed automatically |

Events: `record` (data = record JSON, id = `partition:offset`), `assignment` (with `group`) and `error`. A keep-alive comment is sent every 15 s.

```js
const es = new EventSource('http://localhost:8082/v1/topics/payments/stream?filter=' +
  encodeURIComponent('this.amount > 1000000'));
es.addEventListener('record', e => console.log(JSON.parse(e.data)));
```

### HTTP consumer groups

With HTTP groups the broker does the partition assignment, so clients stay simple.

| Call | Body / query | Response |
|---|---|---|
| `POST /v1/groups/{g}/members` | `{"topics":["orders"],"offset_reset":"earliest","auto_commit":true,"filter":null,"session_timeout_ms":30000}` | `{"group":"g","member_id":"http-..."}` |
| `GET /v1/groups/{g}/members/{m}/records` | `?timeout_ms=1000&max_records=500&max_bytes=` | `{"member_id","generation","assignment":[{"topic","partition"}],"records":[...]}` |
| `POST /v1/groups/{g}/members/{m}/commit` | empty (commits the positions it has delivered) or `{"offsets":[{"topic","partition","offset"}]}` | `{"committed":[...]}` |
| `DELETE /v1/groups/{g}/members/{m}` | — | `{"left":true}` |

A member that stops polling for `session_timeout_ms` is removed and its partitions are rebalanced. Polling after that returns 404 `unknown member`, and the client must join again.

### Share groups

| Call | Body | Response |
|---|---|---|
| `POST /v1/share/{g}/poll` | `{"topics":["jobs"],"member":"w1","max_records":100,"lock_ms":30000,"max_attempts":5,"dlq_topic":"jobs.dlq","filter":null,"offset_reset":"earliest","timeout_ms":1000}` | `{"group","member","records":[{...,"delivery_count":1}]}` |
| `POST /v1/share/{g}/ack` | `{"member":"w1","acks":[{"topic":"jobs","partition":0,"offset":7,"action":"accept"}]}` | `{"results":[true]}` |
| `GET /v1/share/{g}` | — | per-partition `start_offset`, `in_flight` and `acquired` |

`action` is `accept`, `release` (make it available again) or `reject` (send to `dlq_topic`). A record whose lock expires without an ack is redelivered. After `max_attempts` deliveries it goes to the DLQ, with the headers `bigpipe.dlq.topic`, `bigpipe.dlq.partition`, `bigpipe.dlq.offset`, `bigpipe.dlq.group`, `bigpipe.dlq.attempts` and `bigpipe.dlq.reason`.

---

## Admin API (9644)

If `admin_api_key` is set, every call needs `Authorization: Bearer <key>`.

| Method and path | Purpose |
|---|---|
| `GET /v1/cluster` | node id, listeners, shards, object store, defaults, counts |
| `GET /v1/metrics` | JSON snapshot of the counters (rates for the Console) |
| `GET /v1/topics` · `POST /v1/topics` | list; create `{"name","partitions","mode","config":{}}` |
| `GET /v1/topics/{t}` · `DELETE /v1/topics/{t}` | describe (per-partition offsets, bytes per storage tier, effective config); delete |
| `PATCH /v1/topics/{t}/config` | `{"config":{"retention.ms":"86400000","flush.ms":null}}` (`null` resets to the default) |
| `POST /v1/topics/{t}/partitions` | `{"count":12}`: grow to 12 partitions |
| `POST /v1/topics/{t}/migrate` | `{"to":"diskless"}`: online storage migration; returns `switch_offsets` |
| `POST /v1/topics/{t}/compact` | compact a `cleanup.policy=compact` topic now; returns records and bytes before/after per partition |
| `GET /v1/topics/{t}/messages` | message browser: `?partition=&offset=&limit=&filter=` |
| `GET /v1/groups` · `GET /v1/groups/{g}` · `DELETE /v1/groups/{g}` | list, describe (members, committed offsets, lag), delete |
| `POST /v1/groups/{g}/reset` | `{"topic","to":"earliest\|latest\|offset","offset":0,"partitions":[0,1]}`; the group must be empty |
| `GET /v1/share-groups` | share groups with in-flight counts |
| `GET/POST /v1/flows` · `DELETE /v1/flows/{n}` · `POST /v1/flows/{n}/pause`, `/resume` | BigPipe Flow; `POST` takes JSON or YAML (`content-type: application/yaml`) |
| `POST /v1/expr/validate` | `{"expr":"this.amount > 1","kind":"filter\|mapping","sample":{"amount":5}}` → `{"valid":true,"result":true}` |
| `GET /healthz` | liveness |

## Metrics (9645)

`GET /metrics` returns OpenMetrics text: `bp_produce_records_total`, `bp_produce_bytes_total`, `bp_fetch_records_total`, `bp_fetch_bytes_total`, `bp_requests_total`, `bp_request_errors_total`, `bp_http_requests_total`, `bp_connections_open`, `bp_connections_total`, `bp_produce_latency_seconds`, `bp_fetch_latency_seconds` (histograms), `bp_diskless_files_total`, `bp_diskless_bytes_total`, `bp_diskless_put_latency_seconds`, `bp_diskless_files_referenced`, `bp_tiered_uploads_total`, `bp_object_cache_bytes`, `bp_group_rebalances_total`, `bp_compactions_total`, `bp_compaction_removed_records_total` and `bp_topics`.

## AdminApi gateway (9650)

`control-plane/src/BigPipe.AdminApi` sits in front of the admin API and adds:

- **API keys with roles:** `Viewer` (GET), `Operator` (create/alter) and `Admin` (delete, migrate, flows, group resets). Send `Authorization: Bearer <key>` or `X-Api-Key`. If no keys are configured, it runs in open dev mode.
- **Audit trail:** every change is appended to the `__bp_audit` topic (who, what, status, duration, client IP). `GET /v1/audit?limit=100` reads it back.
- `GET /v1/overview` (cluster, topic and group summary), `GET /v1/me`, and OpenAPI at `/openapi/v1.json`.

```json
// appsettings.json
"BigPipe": {
  "AdminUrl": "http://localhost:9644",
  "ApiKeys": [ { "Name": "ci", "Key": "…", "Role": "Operator" } ]
}
```

## Schema Registry (8081)

This is a Confluent-compatible REST API, so existing serializers work. Supported routes: `/subjects`, `/subjects/{s}/versions` (GET/POST), `/subjects/{s}/versions/{v}` and `/schema`, `/subjects/{s}` (lookup and delete), `/schemas/ids/{id}`, `/schemas/types`, `/compatibility/subjects/{s}/versions/{v}` and `/config[/{s}]`. Schema types are `AVRO`, `JSON` and `PROTOBUF`. Compatibility levels are `NONE`, `BACKWARD` (the default), `FORWARD` and `FULL` (plus the `_TRANSITIVE` variants). Schemas are stored in the `__bp_schemas` topic, so the registry itself is stateless.

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*

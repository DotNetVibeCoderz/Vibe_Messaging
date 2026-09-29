# bpql, BigPipe Flow and share groups

[English](bpql-and-flows.md) · [Bahasa Indonesia](../id/bpql-and-flows.md)

## bpql: the expression language

bpql is a small expression language that the broker evaluates for each record. It shows up in four places:

| Where | Example |
|---|---|
| Consumer filters (HTTP fetch, SSE, HTTP groups, `bpctl consume --filter`) | `header("region") == "ID" && this.amount > 1000000` |
| Share-group filters | `this.priority == "high"` |
| Flow `filter` and `route` processors | `"payments." + lower(this.city)` |
| Flow `mapping` processors | `root = this` / `root.card = deleted()` |

An expression is parsed once and then evaluated against each record. Header and key lookups don't allocate. The value is parsed as JSON lazily, and only if `this` is used, so filtering on headers stays very cheap.

### Syntax

| Element | Examples |
|---|---|
| Literals | `42`, `3.14`, `"text"`, `'text'`, `true`, `false`, `null`, `["a","b"]` |
| Fields of the JSON value | `this.amount`, `this.customer.city`, `this.items[0].sku` |
| Comparison | `==`, `!=`, `<`, `<=`, `>`, `>=` |
| Logic | `&&` / `and`, `\|\|` / `or`, `!` / `not` |
| Arithmetic / concatenation | `+`, `-`, `*`, `/`, `%` (`+` concatenates strings) |
| Membership | `this.city in ["Jakarta", "Bandung"]` |

### Functions

| Function | Returns |
|---|---|
| `header(name)` | header value as a string, or `null` |
| `key()` · `value()` / `content()` | key and value as strings |
| `json()` · `json(path)` | the parsed value (same as `this`) or a nested field |
| `topic()` · `partition()` · `offset()` · `timestamp()` · `now()` | record metadata; timestamps in ms |
| `lower(s)` · `upper(s)` · `len(x)` / `length(x)` | string helpers (`len` also works on arrays) |
| `contains(s, sub)` · `starts_with(s, p)` / `startsWith` · `ends_with(s, p)` / `endsWith` | string tests |
| `number(x)` · `string(x)` · `exists(x)` · `coalesce(a, b, ...)` | conversion and null handling |
| `abs(x)` · `round(x[, digits])` · `floor(x)` · `ceil(x)` | math |
| `uuid()` | random UUID (for mappings) |
| `deleted()` | in a mapping, removes the field being assigned |

A missing field is `null`. Comparing `null` with a number is `false`, so `this.amount > 5` simply skips records without `amount` and doesn't fail.

### Mappings

A mapping is a list of assignments, separated by newlines or `;`. It builds a new JSON value called `root`:

```
root = this                          # start from the input
root.total = this.qty * this.price
root.card = deleted()                # drop a sensitive field
root.meta.source = topic()           # nested objects are created as needed
root.id = coalesce(this.id, uuid())
```

To try an expression without deploying anything, use `POST /v1/expr/validate`:

```bash
curl -X POST localhost:9644/v1/expr/validate -H 'content-type: application/json' \
     -d '{"expr":"this.amount > 1000 && header(\"region\") == \"ID\"","sample":{"amount":5000}}'
```

## BigPipe Flow

A **flow** is a managed pipeline that runs inside the broker: input topic → processors → output topic. Use it for routing, cleaning, masking and enrichment that would otherwise need a separate service.

```yaml
apiVersion: bigpipe.io/v1
kind: Flow
metadata:
  name: high-value
spec:
  input:  { bigpipe: { topic: payments } }
  start_from: earliest          # or latest (first run only)
  dlq: payments.flow-errors     # optional: records that fail processing
  pipeline:
    processors:
      - filter: 'this.amount > 5000000'
      - mapping: |
          root = this
          root.card = deleted()
          root.flagged = true
      - route: '"payments.high." + lower(this.city)'   # optional: per-record topic
  output: { bigpipe: { topic: payments.high } }
```

```bash
bpctl flow deploy high-value.yaml
bpctl flow list                 # records in / out / filtered / errors, lag
bpctl flow pause high-value
bpctl flow resume high-value
bpctl flow delete high-value
```

The same spec is accepted as JSON:

```json
{"name":"high-value","input":"payments","output":"payments.high",
 "processors":[{"filter":"this.amount > 5000000"},{"mapping":"root = this\nroot.card = deleted()"}]}
```

How it runs:

- Processors run in order. `filter` drops records, `mapping` rewrites the value, and `route` picks the output topic (topics are auto-created).
- Keys, headers and timestamps are kept as they were.
- Progress is committed as the consumer group `__flow:<name>`. After a restart or a pause, the flow continues where it stopped (at-least-once). You can watch its lag like any other group.
- Definitions are stored in `<data_dir>/meta/flows.json` and restarted with the broker.

![Flows in the Console](../images/console-flows.png)

## Share groups

A share group turns a topic into a **work queue**. Unlike a consumer group, any number of workers can read the same partition. Each record is **locked** to the worker that received it until the worker acknowledges it:

| Ack | Effect |
|---|---|
| `accept` | done; never delivered again |
| `release` | unlocked right away and redelivered to any worker |
| `reject` | sent to the dead-letter topic |
| *(no ack before `lock_ms`)* | the lock expires and the record is redelivered |

Every delivery carries `delivery_count`. When it goes past `max_attempts`, the record goes to `dlq_topic` with diagnostic headers (`bigpipe.dlq.reason`, `bigpipe.dlq.topic`, `bigpipe.dlq.partition`, `bigpipe.dlq.offset`, `bigpipe.dlq.group` and `bigpipe.dlq.attempts`).

```python
async with ShareConsumer(group="email-workers", topics=["email-jobs"],
                         max_attempts=5, dlq_topic="email-jobs.dlq") as worker:
    async for job in worker:
        try:
            await send_email(job.json()); job.accept()
        except TemporaryError:
            job.release()
        except Exception:
            job.reject()
```

Share groups are available over HTTP (`/v1/share/{group}/poll` and `/ack`) and in every SDK. The Gallery case *Share groups: work queues with a DLQ* shows them live.

![Share groups in the Gallery](../images/gallery-share-groups.png)

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*

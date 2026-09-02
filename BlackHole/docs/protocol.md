# The wire format

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Every byte BlackHole sends is written and parsed by exactly one type,
[`FrameCodec`](../src/BlackHole/Protocol/FrameCodec.cs). Both ends of every connection go through
it, which is what makes it impossible for the client and server halves of the protocol to drift
apart.

## One frame

```
 0        4      5      6          8                    16
 +--------+------+------+----------+--------------------+---------+---------+
 | Length | Type | Flags| HdrLen   |  CorrelationId     | Header  | Payload |
 | int32  | u8   | u8   | uint16   |  int64             | UTF-8   |  bytes  |
 +--------+------+------+----------+--------------------+---------+---------+
 \__ 4 __/\_____________ 12 bytes of fixed header ______/
          \____________ Length counts everything from here on _______________/
```

Everything is little-endian.

| Field | Size | Meaning |
|---|---|---|
| `Length` | 4 | Bytes in the rest of the frame. Not included in its own count. |
| `Type` | 1 | [`MessageType`](../src/BlackHole/Protocol/MessageType.cs). The numeric values are protocol, never reuse one. |
| `Flags` | 1 | [`MessageFlags`](../src/BlackHole/Protocol/MessageType.cs): `Error`, `Compressed` (reserved), `NoReply`. |
| `HdrLen` | 2 | UTF-8 length of `Header`. Caps a header at 65,535 bytes. |
| `CorrelationId` | 8 | Matches a reply to its request. Reused as a chunk index and a batch count. |
| `Header` | *HdrLen* | Routing key, UTF-8. |
| `Payload` | remainder | The body. |

The prefix is 16 bytes total, which leaves the payload 8-byte aligned inside the frame and is
**8 bytes smaller per message** than v2's GUID-based header.

### Why these choices

**A 4-byte length prefix, counting only what follows.** A reader needs 4 bytes to know whether it
has a whole frame. Excluding the prefix from its own count means the check is
`buffer.Length >= 4 + length` with no adjustment to get wrong.

**An int64 correlation id, not a GUID.** v2 sent a 16-byte `Guid` per message and called
`Guid.NewGuid()` per request — a cryptographic RNG draw on the hot path. An interlocked counter is
8 bytes, needs no entropy, and is unique for as long as a connection lives, which is the only scope
where correlation means anything.

**A 2-byte header length.** Headers are method names, topics and stream ids. 64 KiB is far past any
sane one, and the two saved bytes keep the fixed header at a tidy 12.

**A UTF-8 header rather than an id.** Topics are hierarchical text and wildcards match on their
segments, so the text has to be on the wire. The cost is paid back by
[`HeaderCache`](../src/BlackHole/Protocol/HeaderCache.cs), which turns the repeated bytes back into
the same `string` instance without allocating.

## What `Header` and `CorrelationId` mean

Both fields are overloaded by `Type`. This is the one place the protocol asks you to hold two ideas
at once, so it is worth stating plainly:

| Type | `Header` | `CorrelationId` |
|---|---|---|
| `RpcRequest` / `RpcResponse` | method name | matches request to reply |
| `Publish` / `Subscribe` / `Unsubscribe` | topic or filter | unused |
| `StreamStart` | stream id | unused |
| `StreamChunk` | stream id | zero-based chunk index |
| `StreamEnd` | stream id | total chunk count |
| `StreamAbort` | stream id | unused; payload is a UTF-8 reason |
| `Batch` | empty | number of inner messages |
| `Ping` / `Pong` | empty | probe id |

## Message types

```
0x01 RpcRequest      0x10 StreamStart      0x20 Batch
0x02 RpcResponse     0x11 StreamChunk      0x30 Ping
0x03 Publish         0x12 StreamEnd        0x31 Pong
0x04 Subscribe       0x13 StreamAbort
0x05 Ack
0x06 Unsubscribe
```

The gaps are deliberate: related types share a high nibble, so a new streaming type gets `0x14`
rather than whatever number happens to be free.

`Ping` and `Pong` are handled inside the transport. They are never routed, so application handlers
never see keepalive traffic.

## Batch envelopes

A `Batch` payload is **a run of complete BlackHole frames** — the same format as above, nested one
level deep.

```
Batch frame
+--------+------+-----+--------+------+  payload:
| Length | 0x20 | ... | HdrLen | Corr |  +--------------+--------------+-----
+--------+------+-----+--------+------+  | inner frame  | inner frame  | ...
                                          +--------------+--------------+-----
```

This is the single most important difference from v2, which invented a *second, shorter* inner
layout with no correlation id. Two formats meant two parsers, and a change to one silently broke the
other. Here `BatchReceiver` unpacks with the same `FrameCodec.TryRead` the transport uses, so there
is nothing to keep in sync.

Nested batches are ignored rather than unpacked: one level only, because a batch containing itself
is a loop waiting to happen.

## StreamStart descriptors

A `StreamStart` payload carries a [`StreamDescriptor`](../src/BlackHole/Protocol/StreamDescriptor.cs)
so the receiver knows what is coming before the first chunk lands:

```
+------------------+----------+---------+----------------+-------------+
| TotalLength (8)  | NameLen  | Name    | ContentTypeLen | ContentType |
| int64, -1 unknown| uint16   | UTF-8   | uint16         | UTF-8       |
+------------------+----------+---------+----------------+-------------+
```

v2 sent a bare stream id, which left a receiver unable to size a buffer, show progress, or decide
where the content should go. The encoding is fixed-order binary rather than JSON to keep
`StreamStart` small and its parsing allocation-light.

## Framing rules

**Reading.** `FrameCodec.TryRead` returns `false` until a whole frame is buffered; it never blocks
and never partially consumes. When the payload happens to sit in one contiguous segment — the common
case — the returned `ReadOnlyMemory<byte>` **points into the transport's buffer**, so the receive
path copies nothing. When a payload straddles segments, the codec rents an array from
`ArrayPool<byte>.Shared` and hands it back through an `out` parameter for the caller to return.

**Payload lifetime.** A received payload is valid only until the dispatch for that message
completes. A handler that keeps the bytes must copy them — `BlackHoleMessage.ToOwned()` does exactly
that. This is the one rule the API cannot enforce for you, and it is the price of a zero-copy
receive path.

**Failures are fatal.** `BlackHoleProtocolException` means the byte stream is not a valid frame
sequence: a negative length, a header that does not fit its frame, a frame past
`MaxFrameLength`. Once framing is lost there is no way to resynchronise, so the connection is
closed rather than guessed at. `MaxFrameLength` (16 MiB by default) is checked *before* any buffer
is sized, so a hostile length prefix cannot make the process allocate.

## Compatibility

The v3 format is **not** compatible with v2 — the fixed header changed shape and batching changed
layout. Both ends must be v3. See [Migrating from v2](migration-v2.md).

---

*Built by Gravicode Studios, led by Kang Fadhil.*

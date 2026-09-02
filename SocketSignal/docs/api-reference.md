# API reference

*Gravicode Studios, led by Kang Fadhil.*

Namespace: `SocketSignal`.

---

## `SocketSignalServer`

Accepts WebSocket clients and routes calls between them. `IAsyncDisposable`.

### Construction

```csharp
new SocketSignalServer(string urlPrefix, SocketSignalOptions? options = null)
```

`urlPrefix` is an `HttpListener` prefix — `http://`, while clients dial `ws://`.

### Properties

| Member | Type | Notes |
|---|---|---|
| `UrlPrefix` | `string` | What the server listens on |
| `Name` | `string` | Announced in the welcome frame. Default `"SocketSignal"` |
| `ClientCount` | `int` | Live connections. Cheap |
| `Clients` | `IReadOnlyCollection<ClientConnection>` | Snapshot. Allocates — prefer `ClientCount` in a loop |
| `GroupNames` | `IReadOnlyCollection<string>` | Groups with at least one member |
| `Methods` | `IReadOnlyCollection<string>` | Registered method names |
| `Statistics` | `SignalStatistics` | Rolled up on read across live and closed connections |
| `Authenticate` | `Func<HttpListenerContext, ValueTask<bool>>?` | Return false to reject the upgrade with 403 |

### Events

| Event | Signature |
|---|---|
| `ClientConnected` | `Action<ClientConnection>` |
| `ClientDisconnected` | `Action<ClientConnection, string>` — the string is the reason |

### Registration

```csharp
void Register(string method, Func<ClientConnection, JsonElement[], Task<object?>> handler)
void Register<TResult>(string method, Func<ClientConnection, ValueTask<TResult>> handler)
void Register<T1, TResult>(string method, Func<ClientConnection, T1?, ValueTask<TResult>> handler)
void Register<T1, T2, TResult>(string method, Func<ClientConnection, T1?, T2?, ValueTask<TResult>> handler)
void Register<T1, T2, T3, TResult>(string method, Func<ClientConnection, T1?, T2?, T3?, ValueTask<TResult>> handler)
bool Unregister(string method)
```

Registering the same name twice replaces the handler. Registration is expected at start-up: it
takes a lock and rebuilds the dispatch table, while lookups on the receive path are lock-free.

Arguments the caller did not supply arrive as `default`.

### Lifetime

```csharp
Task StartAsync(CancellationToken cancellationToken = default)
Task StopAsync()
ValueTask DisposeAsync()
```

`StartAsync` runs the accept loop and does not return while listening.

### Sending

```csharp
Task BroadcastAsync(string method, params object?[] args)
Task SendToClientAsync(string clientId, string method, params object?[] args)
Task SendToGroupAsync(string groupName, string method, params object?[] args)
ValueTask<TResult?> CallClientAsync<TResult>(string clientId, string method, params object?[] args)
```

The three fan-out methods are fire and forget, and a dead client cannot fail a broadcast to the
healthy ones. `CallClientAsync` throws `SocketSignalException` if the client is not connected.

### Groups

```csharp
void AddToGroup(string groupName, string clientId)
void RemoveFromGroup(string groupName, string clientId)
IReadOnlyCollection<string> GroupMembers(string groupName)
IReadOnlyCollection<string> GroupsOf(string clientId)
```

Groups are created on first use and membership is dropped when a client disconnects.

---

## `SocketSignalClient`

Connects to a server and exchanges calls with it. `IAsyncDisposable`.

### Construction

```csharp
new SocketSignalClient(SocketSignalOptions? options = null)
```

### Properties

| Member | Type | Notes |
|---|---|---|
| `ClientId` | `string?` | From the welcome frame. Null before connecting |
| `IsConnected` | `bool` | |
| `Statistics` | `SignalStatistics` | For the current connection; a reconnect resets it |
| `AutoReconnect` | `bool` | Default false |
| `ReconnectDelay` | `TimeSpan` | First backoff step. Default 1s, doubles |
| `MaxReconnectDelay` | `TimeSpan` | Backoff ceiling. Default 30s |

### Events

| Event | Signature |
|---|---|
| `Connected` | `Action<string>` — the assigned client id |
| `Disconnected` | `Action<string>` — the reason |
| `Reconnecting` | `Action<int>` — attempt number |

### Registration

```csharp
void On(string method, Func<JsonElement[], Task<object?>> handler)
void On<TResult>(string method, Func<ValueTask<TResult>> handler)
void On<T1, TResult>(string method, Func<T1?, ValueTask<TResult>> handler)
void On<T1, T2, TResult>(string method, Func<T1?, T2?, ValueTask<TResult>> handler)
void On<T1, T2, T3, TResult>(string method, Func<T1?, T2?, T3?, ValueTask<TResult>> handler)
bool Off(string method)
```

### Connection

```csharp
Task ConnectAsync(Uri serverUri, CancellationToken cancellationToken = default)
Task DisconnectAsync()
ValueTask DisposeAsync()
```

`ConnectAsync` completes when the welcome frame arrives. `DisconnectAsync` closes the socket but
leaves the client reusable; it also turns `AutoReconnect` off.

### Calling

```csharp
ValueTask<TResult?> CallAsync<TResult>(string method, params object?[] args)
ValueTask<TResult?> CallAsync<TArg, TResult>(string method, TArg arg)
ValueTask<JsonElement?> CallAsync(string method, params object?[] args)
ValueTask SendAsync(string method, params object?[] args)
ValueTask SendAsync<TArg>(string method, TArg arg)
```

The two-type-parameter overloads take exactly one argument and never build an `object[]` or box a
value type. Use them on hot paths.

---

## `ClientConnection`

One connected client, as the server sees it. Handlers receive it as their first parameter.

| Member | Type | Notes |
|---|---|---|
| `Id` | `string` | Server-assigned connection id |
| `RemoteEndPoint` | `IPEndPoint?` | Where the client dialled from, when known |
| `ConnectedAtUtc` | `DateTime` | |
| `Items` | `ConcurrentDictionary<string, object?>` | Per-connection application state |
| `Statistics` | `SignalStatistics` | This connection alone |
| `IsOpen` | `bool` | |
| `Groups` | `IReadOnlyCollection<string>` | |

```csharp
ValueTask SendAsync(string method, params object?[] args)
ValueTask SendAsync<TArg>(string method, TArg arg)
ValueTask<TResult?> CallAsync<TResult>(string method, params object?[] args)
ValueTask<JsonElement?> CallAsync(string method, params object?[] args)
void JoinGroup(string groupName)
void LeaveGroup(string groupName)
ValueTask CloseAsync(string reason = "closed by server")
```

---

## `SocketSignalOptions`

| Property | Default | Meaning |
|---|---|---|
| `CallTimeout` | 30s | How long a call waits for a reply. `Timeout.InfiniteTimeSpan` waits forever |
| `KeepAliveInterval` | 15s | Protocol ping interval on an idle connection. Infinite disables |
| `IdleTimeout` | 60s | Silence after which the server drops a connection |
| `MaxMessageSize` | 4 MB | Largest accepted frame. Exceeding it closes the connection |
| `ReceiveBufferSize` | 4 KB | Initial pooled buffer size. Grows to fit and stays grown |
| `MaxConcurrentInvocations` | 64 | Handlers running at once per connection; the backpressure valve |
| `JsonOptions` | `SocketSignalOptions.Default` | camelCase, nulls omitted, web defaults |

`SocketSignalOptions.Default` is a shared `JsonSerializerOptions`. Replace `JsonOptions` to add
converters; do not mutate the shared instance.

---

## `SignalStatistics`

Interlocked counters, safe to read at any time.

| Member | Meaning |
|---|---|
| `FramesSent` / `FramesReceived` | Whole protocol frames |
| `BytesSent` / `BytesReceived` | UTF-8 payload bytes, excluding WebSocket framing |
| `CallsCompleted` | Calls this peer issued that came back with a result |
| `CallsFailed` | Calls that errored, timed out, or lost the socket |

---

## Exceptions

All derive from `SocketSignalException`.

| Exception | Raised when | Extra |
|---|---|---|
| `SignalInvocationException` | The remote handler threw | `Method`, `RemoteMessage` |
| `MethodNotFoundException` | The peer has no such method | `Method` |
| `SignalTimeoutException` | No reply inside `CallTimeout` | `Method`, `Timeout` |
| `SignalConnectionClosedException` | The socket dropped, or the client is not connected | |

---

## Supporting types

Public, but rarely needed directly.

- **`SocketSignal.Protocol.MessageType`** — `Welcome`, `Invoke`, `Result`, `Ping`, `Pong`, `Unknown`.
- **`SocketSignal.Protocol.SignalFrame`** — a `ref struct` view over a received frame. `TryParse`
  decodes one without allocating; every member is a slice of the receive buffer and is only valid
  until the next message.
- **`SocketSignal.Buffers.PooledBufferWriter`** — a growable `IBufferWriter<byte>` over
  `ArrayPool<byte>.Shared`. `Reset` rewinds without releasing, which is what makes a long-lived
  connection allocation free.

## Thread safety

- `SocketSignalServer` and `SocketSignalClient` are safe for concurrent use.
- Sends on one connection are serialised internally, so concurrent `SendAsync` calls cannot
  interleave on the socket.
- Handlers for one connection may run concurrently, up to `MaxConcurrentInvocations`. If a handler
  touches shared state, guard it — ordering between concurrent invocations is not guaranteed.
- A `SignalFrame` must not escape the call that received it.

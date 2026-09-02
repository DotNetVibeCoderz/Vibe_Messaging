# Getting started

*Gravicode Studios, led by Kang Fadhil.*

## Install

```bash
dotnet add package SocketSignal
```

Requires .NET 10.

## A server

```csharp
using SocketSignal;

var server = new SocketSignalServer("http://localhost:8080/ws/");
server.Register<int, int, int>("sum", (client, a, b) => ValueTask.FromResult(a + b));

await server.StartAsync();   // runs until the token is cancelled
```

Note the prefix is `http://`, not `ws://`. `SocketSignalServer` is built on `HttpListener`, whose
prefixes are HTTP; clients dial the matching `ws://` address. On Windows, any prefix other than
`localhost` needs a URL ACL:

```powershell
netsh http add urlacl url=http://+:8080/ws/ user=DOMAIN\user
```

`StartAsync` does not return while the server is listening, so start it in the background if the
same process does other work:

```csharp
using var cts = new CancellationTokenSource();
_ = server.StartAsync(cts.Token);
```

## A client

```csharp
var client = new SocketSignalClient();
await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

int total = await client.CallAsync<int>("sum", 5, 7);
```

`ConnectAsync` returns once the server's `welcome` frame has arrived, so `client.ClientId` is
populated by the time it completes.

## Registering methods

There are two shapes. The typed one is the one to reach for:

```csharp
// Arguments deserialise straight into their target types.
server.Register<int, int, int>("sum", (client, a, b) => ValueTask.FromResult(a + b));
server.Register<Order, bool>("submit", async (client, order) => await Save(order!));
server.Register<int>("count", client => ValueTask.FromResult(Registry.Count));
```

The untyped shape hands you the raw arguments, and is what to use when the shape varies:

```csharp
server.Register("echo", async (client, args) =>
{
    string? text = args[0].GetString();
    return $"echo:{text}";
});
```

The client side is the same, without the `ClientConnection` parameter:

```csharp
client.On<string, string>("serverHello", text => ValueTask.FromResult($"heard {text}"));
client.On("anything", async args => { /* JsonElement[] */ return null; });
```

Up to three typed arguments are supported. For more, take one object:

```csharp
public record PlaceOrder(string Sku, int Quantity, string Note, bool Rush);

server.Register<PlaceOrder, string>("orders.place", (client, order) => ...);
```

## Calling

```csharp
// Wait for the return value.
int total = await client.CallAsync<int>("sum", 5, 7);

// One typed argument: no object[], no boxing. The fast path for hot calls.
var quote = await client.CallAsync<Symbol, Quote>("quote", symbol);

// No reply wanted.
await client.SendAsync("log", "written to the deck log");

// The v1 shape, still supported.
JsonElement? raw = await client.CallAsync("sum", 5, 7);
```

## Talking to clients

```csharp
await server.BroadcastAsync("tick", 42);                       // everyone
await server.SendToClientAsync(id, "tick", 42);                // one, by id
await server.SendToGroupAsync("operators", "tick", 42);        // a group
int answer = await server.CallClientAsync<int>(id, "double", 21);   // one, with a result
```

Inside a handler, the `ClientConnection` is the caller, so a method can answer them directly:

```csharp
server.Register<string, bool>("subscribe", async (client, topic) =>
{
    client.JoinGroup(topic!);
    await client.SendAsync("subscribed", topic);
    return true;
});
```

## Failure

Calls fail rather than hang. The three things that can go wrong each have their own exception:

```csharp
try
{
    var result = await client.CallAsync<int>("sum", 5, 7);
}
catch (MethodNotFoundException)      { /* no such method on the peer */ }
catch (SignalInvocationException ex) { /* the handler threw: ex.RemoteMessage */ }
catch (SignalTimeoutException)       { /* no reply inside CallTimeout */ }
catch (SignalConnectionClosedException) { /* the socket dropped mid-call */ }
```

All four derive from `SocketSignalException`, so one `catch` covers them when you do not care
which it was.

## Options

```csharp
var options = new SocketSignalOptions
{
    CallTimeout = TimeSpan.FromSeconds(10),
    KeepAliveInterval = TimeSpan.FromSeconds(15),
    IdleTimeout = TimeSpan.FromSeconds(60),
    MaxMessageSize = 4 * 1024 * 1024,
    MaxConcurrentInvocations = 64,
};

var server = new SocketSignalServer("http://localhost:8080/ws/", options);
var client = new SocketSignalClient(options);
```

`Timeout.InfiniteTimeSpan` disables `CallTimeout` and `KeepAliveInterval` individually. Full
descriptions in the [API reference](api-reference.md#socketsignaloptions).

## Reconnecting

Off by default, because a client that silently reconnects also silently loses whatever state the
server was holding for it — group membership included.

```csharp
var client = new SocketSignalClient { AutoReconnect = true };

client.Connected += id => Console.WriteLine($"connected as {id}");
client.Disconnected += why => Console.WriteLine($"lost: {why}");
client.Reconnecting += attempt => Console.WriteLine($"attempt {attempt}");
```

Rejoin groups in the `Connected` handler — the server has no memory of the old connection.

## Authenticating

`Authenticate` runs before the WebSocket upgrade is accepted. Return false to reject with 403.

```csharp
server.Authenticate = context =>
{
    string? token = context.Request.QueryString["token"];
    return ValueTask.FromResult(IsValid(token));
};
```

To carry the identity through, stash it on the connection once it is up:

```csharp
server.ClientConnected += client => client.Items["user"] = LookupUser(client);
```

## Shutting down

Both types are `IAsyncDisposable`. Disposing closes live connections and fails anything still
waiting on them.

```csharp
await using var server = new SocketSignalServer("http://localhost:8080/ws/");
await using var client = new SocketSignalClient();
```

## Next

- [Protocol](protocol.md) — the wire format
- [API reference](api-reference.md) — every public member
- [Performance](performance.md) — the fast paths, and when they matter
- [Client SDKs](clients.md) — Python, Go, Node.js, the browser

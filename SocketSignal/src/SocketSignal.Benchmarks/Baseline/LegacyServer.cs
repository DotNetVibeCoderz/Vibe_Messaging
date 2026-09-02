// Recovered from git history: this is the v1 implementation, kept only so the benchmarks
// can measure the rewrite against something real rather than against a claim.
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;

namespace SocketSignal.Benchmarks.Legacy;

public class LegacySocketSignalServer
{
    private readonly HttpListener _listener;
    private readonly ConcurrentDictionary<string, LegacyClientConnection> _clients = new();
    private readonly ConcurrentDictionary<string, HashSet<string>> _groups = new();
    private readonly ConcurrentDictionary<string, Func<LegacyClientConnection, JsonElement[], Task<object?>>> _handlers = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement?>> _pendingCalls = new();

    public LegacySocketSignalServer(string urlPrefix)
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add(urlPrefix);
    }

    public IReadOnlyCollection<LegacyClientConnection> Clients => _clients.Values.ToList();

    public void Register(string method, Func<LegacyClientConnection, JsonElement[], Task<object?>> handler)
    {
        _handlers[method] = handler;
    }

    public void AddToGroup(string groupName, string clientId)
    {
        var set = _groups.GetOrAdd(groupName, _ => new HashSet<string>());
        lock (set)
        {
            set.Add(clientId);
        }
    }

    public void RemoveFromGroup(string groupName, string clientId)
    {
        if (_groups.TryGetValue(groupName, out var set))
        {
            lock (set)
            {
                set.Remove(clientId);
            }
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        while (!cancellationToken.IsCancellationRequested)
        {
            var ctx = await _listener.GetContextAsync();
            if (!ctx.Request.IsWebSocketRequest)
            {
                ctx.Response.StatusCode = 400;
                ctx.Response.Close();
                continue;
            }

            var wsContext = await ctx.AcceptWebSocketAsync(null);
            _ = HandleClientAsync(wsContext.WebSocket, cancellationToken);
        }
    }

    private async Task HandleClientAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid().ToString("N");
        var client = new LegacyClientConnection(id, socket, this);
        _clients[id] = client;

        // Send welcome with client id
        await client.SendAsync(new LegacySignalMessage
        {
            Type = "welcome",
            Id = id
        }, cancellationToken);

        try
        {
            while (socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                var msg = await client.ReceiveAsync(cancellationToken);
                if (msg == null) break;

                if (msg.Type == "invoke" && msg.Method != null)
                {
                    if (_handlers.TryGetValue(msg.Method, out var handler))
                    {
                        try
                        {
                            var args = msg.Args?.ToArray() ?? Array.Empty<JsonElement>();
                            var resultObj = await handler(client, args);
                            if (msg.ExpectReturn && msg.Id != null)
                            {
                                var resultElement = JsonSerializer.SerializeToElement(resultObj, LegacyClientConnection.JsonOptions);
                                await client.SendAsync(new LegacySignalMessage
                                {
                                    Type = "result",
                                    Id = msg.Id,
                                    Result = resultElement
                                }, cancellationToken);
                            }
                        }
                        catch (Exception ex)
                        {
                            if (msg.ExpectReturn && msg.Id != null)
                            {
                                await client.SendAsync(new LegacySignalMessage
                                {
                                    Type = "result",
                                    Id = msg.Id,
                                    Error = ex.Message
                                }, cancellationToken);
                            }
                        }
                    }
                    else if (msg.ExpectReturn && msg.Id != null)
                    {
                        await client.SendAsync(new LegacySignalMessage
                        {
                            Type = "result",
                            Id = msg.Id,
                            Error = $"Method '{msg.Method}' not found"
                        }, cancellationToken);
                    }
                }
                else if (msg.Type == "result" && msg.Id != null)
                {
                    if (_pendingCalls.TryRemove(msg.Id, out var tcs))
                    {
                        tcs.TrySetResult(msg.Result);
                    }
                }
            }
        }
        finally
        {
            _clients.TryRemove(id, out _);
            foreach (var kv in _groups)
            {
                lock (kv.Value)
                {
                    kv.Value.Remove(id);
                }
            }
            try { socket.Abort(); } catch { }
        }
    }

    public async Task BroadcastAsync(string method, params object?[] args)
    {
        var tasks = _clients.Values.Select(c => c.InvokeClientAsync(method, args, false));
        await Task.WhenAll(tasks);
    }

    public async Task SendToClientAsync(string clientId, string method, params object?[] args)
    {
        if (_clients.TryGetValue(clientId, out var client))
        {
            await client.InvokeClientAsync(method, args, false);
        }
    }

    public async Task SendToGroupAsync(string groupName, string method, params object?[] args)
    {
        if (_groups.TryGetValue(groupName, out var set))
        {
            List<LegacyClientConnection> targets;
            lock (set)
            {
                targets = set
                    .Select(id => _clients.TryGetValue(id, out var c) ? c : null)
                    .Where(c => c != null)
                    .Cast<LegacyClientConnection>()
                    .ToList();
            }
            var tasks = targets.Select(c => c.InvokeClientAsync(method, args, false));
            await Task.WhenAll(tasks);
        }
    }

    internal Task<JsonElement?> InvokeClientAndWaitAsync(string clientId, string method, object?[] args, CancellationToken ct)
    {
        if (!_clients.TryGetValue(clientId, out var client))
            throw new InvalidOperationException("Client not found");

        var callId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<JsonElement?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingCalls[callId] = tcs;
        _ = client.InvokeClientAsync(method, args, true, callId, ct);
        return tcs.Task;
    }
}

public class LegacyClientConnection
{
    private readonly WebSocket _socket;
    private readonly LegacySocketSignalServer _server;

    public string Id { get; }

    public LegacyClientConnection(string id, WebSocket socket, LegacySocketSignalServer server)
    {
        Id = id;
        _socket = socket;
        _server = server;
    }

    public async Task InvokeClientAsync(string method, object?[] args, bool expectReturn, string? callId = null, CancellationToken ct = default)
    {
        var msg = new LegacySignalMessage
        {
            Type = "invoke",
            Id = callId ?? (expectReturn ? Guid.NewGuid().ToString("N") : null),
            Method = method,
            Args = args.Select(a => JsonSerializer.SerializeToElement(a, JsonOptions)).ToArray(),
            ExpectReturn = expectReturn
        };
        await SendAsync(msg, ct);
    }

    public Task SendAsync(LegacySignalMessage msg, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(msg, JsonOptions);
        var buffer = Encoding.UTF8.GetBytes(json);
        return _socket.SendAsync(buffer, WebSocketMessageType.Text, true, ct);
    }

    public async Task<LegacySignalMessage?> ReceiveAsync(CancellationToken ct = default)
    {
        var buffer = new byte[4096];
        using var ms = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await _socket.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);

        var json = Encoding.UTF8.GetString(ms.ToArray());
        return JsonSerializer.Deserialize<LegacySignalMessage>(json, JsonOptions);
    }

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}

public class LegacySignalMessage
{
    public string Type { get; set; } = "";
    public string? Id { get; set; }
    public string? Method { get; set; }
    public JsonElement[]? Args { get; set; }
    public bool ExpectReturn { get; set; }
    public JsonElement? Result { get; set; }
    public string? Error { get; set; }
}

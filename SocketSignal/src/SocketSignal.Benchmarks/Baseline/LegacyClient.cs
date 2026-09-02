// Recovered from git history: this is the v1 implementation, kept only so the benchmarks
// can measure the rewrite against something real rather than against a claim.
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;

namespace SocketSignal.Benchmarks.Legacy;

public class LegacySocketSignalClient
{
    private readonly ClientWebSocket _ws = new();
    private readonly ConcurrentDictionary<string, Func<JsonElement[], Task<object?>>> _handlers = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement?>> _pendingCalls = new();
    private CancellationTokenSource? _cts;

    public string? ClientId { get; private set; }

    public void On(string method, Func<JsonElement[], Task<object?>> handler)
    {
        _handlers[method] = handler;
    }

    public async Task ConnectAsync(Uri serverUri, CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await _ws.ConnectAsync(serverUri, ct);
        _ = ReceiveLoop(_cts.Token);
    }

    public async Task<JsonElement?> CallAsync(string method, params object?[] args)
    {
        var callId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<JsonElement?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingCalls[callId] = tcs;

        await SendAsync(new LegacySignalMessage
        {
            Type = "invoke",
            Id = callId,
            Method = method,
            Args = args.Select(a => JsonSerializer.SerializeToElement(a, JsonSerializerOptions)).ToArray(),
            ExpectReturn = true
        });

        return await tcs.Task;
    }

    public Task SendAsync(string method, params object?[] args)
    {
        return SendAsync(new LegacySignalMessage
        {
            Type = "invoke",
            Id = Guid.NewGuid().ToString("N"),
            Method = method,
            Args = args.Select(a => JsonSerializer.SerializeToElement(a, JsonSerializerOptions)).ToArray(),
            ExpectReturn = false
        });
    }

    private async Task SendAsync(LegacySignalMessage msg)
    {
        var json = JsonSerializer.Serialize(msg, JsonSerializerOptions);
        var buffer = Encoding.UTF8.GetBytes(json);
        await _ws.SendAsync(buffer, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private async Task ReceiveLoop(CancellationToken ct)
    {
        while (_ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            var msg = await ReceiveAsync(ct);
            if (msg == null) break;

            if (msg.Type == "welcome" && msg.Id != null)
            {
                ClientId = msg.Id;
                continue;
            }

            if (msg.Type == "invoke" && msg.Method != null)
            {
                if (_handlers.TryGetValue(msg.Method, out var handler))
                {
                    try
                    {
                        var args = msg.Args?.ToArray() ?? Array.Empty<JsonElement>();
                        var resultObj = await handler(args);
                        if (msg.ExpectReturn && msg.Id != null)
                        {
                            var resultElement = JsonSerializer.SerializeToElement(resultObj, JsonSerializerOptions);
                            await SendAsync(new LegacySignalMessage
                            {
                                Type = "result",
                                Id = msg.Id,
                                Result = resultElement
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        if (msg.ExpectReturn && msg.Id != null)
                        {
                            await SendAsync(new LegacySignalMessage
                            {
                                Type = "result",
                                Id = msg.Id,
                                Error = ex.Message
                            });
                        }
                    }
                }
                else if (msg.ExpectReturn && msg.Id != null)
                {
                    await SendAsync(new LegacySignalMessage
                    {
                        Type = "result",
                        Id = msg.Id,
                        Error = $"Method '{msg.Method}' not found"
                    });
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

    private async Task<LegacySignalMessage?> ReceiveAsync(CancellationToken ct = default)
    {
        var buffer = new byte[4096];
        using var ms = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await _ws.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);

        var json = Encoding.UTF8.GetString(ms.ToArray());
        return JsonSerializer.Deserialize<LegacySignalMessage>(json, JsonSerializerOptions);
    }

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}

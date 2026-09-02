// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
//
// The reference peer for the Python, Go and Node.js client SDKs. Each SDK's test suite starts this
// process, points at the port it prints, and asserts against a server that is the real library -
// not a mock of it. That is the only way a second implementation of a wire format stays honest.
using System.Net;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.InteropServer;

internal static class Program
{
    /// <summary>
    /// One server-side caller per connection, so the <c>callback</c> method can call back into the
    /// client that invoked it. Keyed by transport id.
    /// </summary>
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Patterns.RpcClient> Callers =
        new(StringComparer.Ordinal);

    private static async Task Main(string[] args)
    {
        int port = ArgValue(args, "--port", 0);
        string? pipeName = StringArg(args, "--pipe");
        string? unixPath = StringArg(args, "--unix");

        var options = new TransportOptions
        {
            // Keepalive stays on: a client SDK that cannot answer a Ping is broken, and this is
            // where that should surface.
            KeepAliveInterval = TimeSpan.FromSeconds(5),
            ErrorHandler = ex => Console.Error.WriteLine($"[transport] {ex.GetType().Name}: {ex.Message}"),
        };

        // The client SDKs test against whichever transport they are exercising; the protocol above
        // the transport is identical, which is the point.
        IListenerHost listener = pipeName is not null
            ? new NamedPipeListenerHost(pipeName, options)
            : unixPath is not null
                ? new UnixSocketListenerHost(unixPath, options)
                : new TcpListenerHost(new IPEndPoint(IPAddress.Loopback, port), options);

        await using var server = new BlackHoleServer(listener, options);

        ConfigureMethods(server);
        ConfigureConnections(server);

        server.Start();

        // The test harness reads this line to learn the port, so it must be the first thing out and
        // it must be flushed.
        // TCP reports its port so a harness can find it; the others report the endpoint they were
        // given, so one READY line works for every transport.
        Console.WriteLine(listener is TcpListenerHost tcp
            ? $"READY {tcp.EndPoint.Port}"
            : $"READY {server.Endpoint}");
        Console.Out.Flush();

        using var stop = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };

        try
        {
            await Task.Delay(Timeout.Infinite, stop.Token);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>The contract every client SDK's test suite is written against.</summary>
    private static void ConfigureMethods(BlackHoleServer server)
    {
        server.Rpc
            // Returns the request bytes unchanged. Proves framing round-trips byte for byte.
            .Register("echo", request => request.Payload)

            // Proves UTF-8 handling in both directions, including non-ASCII.
            .RegisterText("upper", text => text.ToUpperInvariant())

            // Proves numeric payloads survive: sums the bytes and returns an int32 little-endian.
            .Register("sum", request =>
            {
                int total = 0;
                foreach (byte b in request.Payload.Span) total += b;
                return BitConverter.GetBytes(total);
            })

            // Proves the client surfaces a server-side failure instead of hanging.
            .Register("boom", _ => throw new InvalidOperationException("interop server raised boom"))

            // Proves the client's own timeout fires. Never replies.
            .Register("sleep", async (request, ct) =>
            {
                int ms = int.TryParse(request.Text(), out int parsed) ? parsed : 30_000;
                await Task.Delay(ms, ct);
                return ReadOnlyMemory<byte>.Empty;
            })

            // Proves large payloads cross intact in both directions.
            .Register("big", request =>
            {
                int size = int.TryParse(request.Text(), out int parsed) ? parsed : 1024;
                var buffer = new byte[size];
                for (int i = 0; i < size; i++) buffer[i] = (byte)(i % 251);
                return buffer;
            })

            // Proves the client can be called by the server: asks the client to identify itself and
            // returns whatever it said. Detached because it awaits a reply that only this
            // connection's receive loop can deliver - a normal handler would block that loop and
            // deadlock. The caller is created per connection at accept time, since an RpcClient
            // only completes calls whose responses are routed back to it.
            .RegisterDetached("callback", async (request, ct) =>
            {
                if (!Callers.TryGetValue(request.Transport.Id, out Patterns.RpcClient? caller))
                    return "no caller registered for this connection"u8.ToArray();

                string reply = await caller.CallTextAsync("client/identify", request.Text(), cancellationToken: ct);
                return Encoding.UTF8.GetBytes(reply);
            });
    }

    private static void ConfigureConnections(BlackHoleServer server)
    {
        server.ClientConnected += connection =>
        {
            Console.Error.WriteLine($"[server] {connection.Transport.Id} connected");

            // Responses to the server's own callbacks must reach the RpcClient that made them, so
            // the caller is built here and routed before any traffic arrives.
            var caller = new Patterns.RpcClient(connection.Transport) { DefaultTimeout = TimeSpan.FromSeconds(5) };
            connection.Router.On(MessageType.RpcResponse, caller.HandleAsync);
            Callers[connection.Transport.Id] = caller;

            // A completed upload is echoed back on "stream/done" as "<id>:<length>", so a client can
            // assert its stream arrived without needing a second channel.
            connection.Streams.Completed += async (_, e) =>
            {
                Console.Error.WriteLine($"[server] stream '{e.StreamId}' complete: {e.Length:N0} bytes");
                try
                {
                    await connection.SendAsync(new BlackHoleMessage(
                        MessageType.Publish, "stream/done",
                        Encoding.UTF8.GetBytes($"{e.StreamId}:{e.Length}")));
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[server] could not confirm stream: {ex.Message}");
                }
            };

            connection.Streams.Aborted += (id, reason) =>
                Console.Error.WriteLine($"[server] stream '{id}' aborted: {reason}");
        };

        server.ClientDisconnected += (connection, failure) =>
        {
            if (Callers.TryRemove(connection.Transport.Id, out Patterns.RpcClient? caller))
                caller.Dispose();
            Console.Error.WriteLine($"[server] {connection.Transport.Id} gone{(failure is null ? "" : $": {failure.Message}")}");
        };

        server.HandlerFaulted += (message, ex) =>
            Console.Error.WriteLine($"[server] handler failed on {message.Type} '{message.Header}': {ex.Message}");
    }

    private static string? StringArg(string[] args, string name)
    {
        int index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }

    private static int ArgValue(string[] args, string name, int fallback)
    {
        int index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length && int.TryParse(args[index + 1], out int value)
            ? value
            : fallback;
    }
}

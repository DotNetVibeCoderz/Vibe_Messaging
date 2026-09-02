// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
// A single process that runs the server and a client, exercising every pattern in order.
using System.Diagnostics;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Demo;

internal static class Program
{
    private const int Port = 5000;
    private const string Host = "127.0.0.1";

    private static async Task Main(string[] args)
    {
        Banner();

        int rpcCalls = ArgValue(args, "--rpc", 20_000);
        int batchSize = ArgValue(args, "--batch", 5_000);
        int streamBytes = ArgValue(args, "--stream", 8 * 1024 * 1024);

        var options = new TransportOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(15),
            ErrorHandler = ex => Console.WriteLine($"  [transport] {ex.GetType().Name}: {ex.Message}"),
        };

        await using var server = new BlackHoleServer(Port, options);
        ConfigureServer(server);
        server.Start();
        Section("Server");
        Console.WriteLine($"  listening on {server.EndPoint}");

        await using BlackHoleClient client = await BlackHoleClient.ConnectWithRetryAsync(Host, Port, options: options);
        Console.WriteLine($"  client connected from {client.Transport.RemoteEndPoint}");

        await RunRpcAsync(client, rpcCalls);
        await RunPubSubAsync(client);
        await RunStreamingAsync(client, streamBytes);
        await RunBatchingAsync(client, batchSize);
        await RunCallbackAsync(client, server);

        Section("Connection statistics");
        Diagnostics.StatisticsSnapshot stats = client.Statistics.Snapshot();
        Console.WriteLine($"  {stats}");
        Console.WriteLine($"  keepalive round trip : {stats.LastRoundTrip?.TotalMilliseconds.ToString("F3") ?? "not yet measured"} ms");
        Console.WriteLine($"  header cache         : {((TcpTransport)client.Transport).HeaderCache.Hits:N0} hits / " +
                          $"{((TcpTransport)client.Transport).HeaderCache.Misses:N0} misses");

        Console.WriteLine();
        Console.WriteLine("Done. Built by Gravicode Studios, led by Kang Fadhil.");
        if (!Console.IsInputRedirected && args.Contains("--wait"))
        {
            Console.WriteLine("[Press Enter to exit]");
            Console.ReadLine();
        }
    }

    private static void ConfigureServer(BlackHoleServer server)
    {
        server.Rpc
            .Register("echo", request => request.Payload)
            .RegisterText("upper", text => text.ToUpperInvariant())
            .RegisterText("time", _ => DateTimeOffset.UtcNow.ToString("O"))
            .Register("sum", request =>
            {
                int total = 0;
                foreach (byte b in request.Payload.Span) total += b;
                return BitConverter.GetBytes(total);
            });

        server.ClientConnected += connection =>
        {
            Console.WriteLine($"  [server] {connection.Transport.Id} connected from {connection.Transport.RemoteEndPoint}");

            connection.Streams.Started += (id, descriptor) =>
                Console.WriteLine($"  [server] stream '{id}' opening: {descriptor.Name}, " +
                                  $"{(descriptor.HasLength ? $"{descriptor.TotalLength:N0} bytes" : "length unknown")}");

            connection.Streams.Completed += (_, e) =>
                Console.WriteLine($"  [server] stream '{e.StreamId}' complete: {e.Length:N0} bytes reassembled");

            connection.Streams.Aborted += (id, reason) =>
                Console.WriteLine($"  [server] stream '{id}' aborted: {reason}");
        };

        server.ClientDisconnected += (connection, failure) =>
            Console.WriteLine($"  [server] {connection.Transport.Id} disconnected{(failure is null ? "" : $": {failure.Message}")}");

        server.HandlerFaulted += (message, ex) =>
            Console.WriteLine($"  [server] handler failed on {message.Type}: {ex.Message}");
    }

    // ------------------------------------------------------------------ RPC

    private static async Task RunRpcAsync(BlackHoleClient client, int calls)
    {
        Section("RPC");

        string upper = await client.Rpc.CallTextAsync("upper", "halo blackhole");
        Console.WriteLine($"  upper(\"halo blackhole\") -> \"{upper}\"");

        try
        {
            await client.Rpc.CallAsync("does-not-exist", timeout: TimeSpan.FromSeconds(2));
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"  unknown method fails fast -> {ex.Message}");
        }

        byte[] payload = Encoding.UTF8.GetBytes("Speed Check");
        await client.Rpc.CallAsync("echo", payload); // warm up JIT and the header cache

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < calls; i++)
            await client.Rpc.CallAsync("echo", payload);
        sw.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        Console.WriteLine($"  {calls:N0} sequential round trips in {sw.Elapsed.TotalMilliseconds:N1} ms");
        Console.WriteLine($"  {calls / sw.Elapsed.TotalSeconds:N0} calls/sec, " +
                          $"{sw.Elapsed.TotalMilliseconds * 1000 / calls:N1} us per round trip");
        Console.WriteLine($"  {allocated / (double)calls:N0} bytes allocated per call (both ends, in this process)");
    }

    // --------------------------------------------------------------- Pub/Sub

    private static async Task RunPubSubAsync(BlackHoleClient client)
    {
        Section("Pub/Sub");

        var received = new List<string>();
        var gate = new SemaphoreSlim(0);
        client.PubSub.Received += (topic, payload) =>
        {
            lock (received) received.Add($"{topic} = {Encoding.UTF8.GetString(payload.Span)}");
            gate.Release();
        };

        await client.PubSub.SubscribeAsync("sensor/+/temperature");
        await client.PubSub.SubscribeAsync("alarm/#");
        await Task.Delay(100);

        await client.PubSub.PublishAsync("sensor/tank-3/temperature", "28.4");
        await client.PubSub.PublishAsync("sensor/tank-9/temperature", "31.0");
        await client.PubSub.PublishAsync("sensor/tank-3/humidity", "62");   // no filter matches
        await client.PubSub.PublishAsync("alarm/floor-1/pump", "overheating");

        for (int i = 0; i < 3 && await gate.WaitAsync(TimeSpan.FromSeconds(2)); i++) { }

        lock (received)
        {
            Console.WriteLine($"  subscribed to sensor/+/temperature and alarm/#");
            foreach (string line in received)
                Console.WriteLine($"  delivered: {line}");
            Console.WriteLine("  sensor/tank-3/humidity was published but matched no filter, as intended");
        }
    }

    // -------------------------------------------------------------- Streaming

    private static async Task RunStreamingAsync(BlackHoleClient client, int totalBytes)
    {
        Section("Streaming");

        var payload = new byte[totalBytes];
        Random.Shared.NextBytes(payload);

        var progress = new Progress<long>(sent =>
        {
            if (sent % (2 * 1024 * 1024) < 64 * 1024)
                Console.WriteLine($"  sent {sent / (1024.0 * 1024):N1} MiB");
        });

        var sw = Stopwatch.StartNew();
        long sent = await client.OutgoingStreams.SendAsync(
            "firmware-2026.bin",
            payload,
            new StreamDescriptor("firmware-2026.bin", payload.Length, "application/octet-stream"),
            chunkSize: 16 * 1024,
            progress: progress);
        sw.Stop();

        Console.WriteLine($"  {sent / (1024.0 * 1024):N1} MiB in {sw.Elapsed.TotalMilliseconds:N0} ms " +
                          $"({sent / (1024.0 * 1024) / sw.Elapsed.TotalSeconds:N0} MiB/sec)");
        await Task.Delay(200); // let the server print its completion line
    }

    // --------------------------------------------------------------- Batching

    private static async Task RunBatchingAsync(BlackHoleClient client, int count)
    {
        Section("Batching");

        var messages = new BlackHoleMessage[count];
        for (int i = 0; i < count; i++)
            messages[i] = new BlackHoleMessage(MessageType.Publish, "log/entry", Encoding.UTF8.GetBytes($"Log Data {i}"));

        var sw = Stopwatch.StartNew();
        await client.Batch.SendBatchAsync(messages);
        sw.Stop();

        Console.WriteLine($"  {count:N0} messages in {client.Batch.BatchesSent} envelope(s), " +
                          $"{sw.Elapsed.TotalMilliseconds:N1} ms");
        Console.WriteLine($"  one socket write instead of {count:N0}");
    }

    // ------------------------------------------------------- Server callbacks

    private static async Task RunCallbackAsync(BlackHoleClient client, BlackHoleServer server)
    {
        Section("Server-to-client RPC");

        client.Handlers.RegisterText("device/status", _ => "ok: 4 sensors online");

        BlackHoleConnection connection = server.Connections.First();
        var caller = new Patterns.RpcClient(connection.Transport);
        connection.Router.On(MessageType.RpcResponse, caller.HandleAsync);

        string status = await caller.CallTextAsync("device/status", "?");
        Console.WriteLine($"  server called the client back -> \"{status}\"");
        caller.Dispose();
    }

    // ---------------------------------------------------------------- console

    private static void Banner()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine("==================================================");
        Console.WriteLine("  BLACKHOLE MESSAGING v3 - .NET 10");
        Console.WriteLine("  RPC / Pub-Sub / Streaming / Batching over TCP");
        Console.WriteLine("  Gravicode Studios - led by Kang Fadhil");
        Console.WriteLine("==================================================");
    }

    private static void Section(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"--- {title} ".PadRight(50, '-'));
    }

    private static int ArgValue(string[] args, string name, int fallback)
    {
        int index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length && int.TryParse(args[index + 1], out int value)
            ? value
            : fallback;
    }
}

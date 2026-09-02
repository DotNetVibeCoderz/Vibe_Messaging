// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
// A server and two clients in one process, walking through everything the library does.
using System.Diagnostics;
using SocketSignal;

const string Http = "http://localhost:8080/ws/";
const string Ws = "ws://localhost:8080/ws/";

using var cts = new CancellationTokenSource();
Console.WriteLine("SocketSignal demo - Gravicode Studios, led by Kang Fadhil");
Console.WriteLine();

// "serve" runs the server and nothing else, so the Python, Go and Node examples in clients/ have
// something to talk to.
bool serveOnly = args.Length > 0 && args[0].Equals("serve", StringComparison.OrdinalIgnoreCase);

// -------------------------------------------------------------------------------------------
// 1. The server, and the methods clients may call on it.
// -------------------------------------------------------------------------------------------
await using var server = new SocketSignalServer(Http) { Name = "demo-station" };

// Typed registration: arguments are deserialised straight into int, no JsonElement in sight.
server.Register<int, int, int>("sum", (_, a, b) => ValueTask.FromResult(a + b));

// The v1 shape still works, for handlers that want the raw arguments.
server.Register("echo", async (client, args) =>
{
    await Task.Yield();
    string? text = args[0].GetString();
    Console.WriteLine($"  server  <- echo from {Short(client.Id)}: {text}");
    return $"echo:{text}";
});

// A handler can put the caller into a group and talk back to it.
server.Register<string, bool>("join", (client, group) =>
{
    client.JoinGroup(group!);
    Console.WriteLine($"  server  <- {Short(client.Id)} joined '{group}'");
    return ValueTask.FromResult(true);
});

// Errors travel: this one throws on purpose.
server.Register<string, string>("explode", (_, _) =>
    throw new InvalidOperationException("reactor offline"));

server.ClientConnected += c => Console.WriteLine($"  server  ++ {Short(c.Id)} connected from {c.RemoteEndPoint}");
server.ClientDisconnected += (c, why) => Console.WriteLine($"  server  -- {Short(c.Id)} left: {why}");

_ = server.StartAsync(cts.Token);
Console.WriteLine($"Server listening on {Ws}");
Console.WriteLine();

if (serveOnly)
{
    Console.WriteLine("Serving. Registered methods: " + string.Join(", ", server.Methods));
    Console.WriteLine("Press Ctrl+C to stop.");
    await Task.Delay(Timeout.Infinite, cts.Token);
    return;
}

// -------------------------------------------------------------------------------------------
// 2. Two clients, so groups and broadcast have somewhere to land.
// -------------------------------------------------------------------------------------------
await using var alice = new SocketSignalClient();
await using var bob = new SocketSignalClient();

foreach ((SocketSignalClient client, string name) in new[] { (alice, "alice"), (bob, "bob") })
{
    // Methods the server may call on this client.
    client.On<string, string>("serverHello", text =>
    {
        Console.WriteLine($"  {name,-7} <- serverHello: {text}");
        return ValueTask.FromResult($"{name} heard you");
    });

    client.On<int, int>("double", n => ValueTask.FromResult(n * 2));

    await client.ConnectAsync(new Uri(Ws), cts.Token);
    Console.WriteLine($"  {name,-7} connected as {Short(client.ClientId!)}");
}
Console.WriteLine();

// -------------------------------------------------------------------------------------------
// 3. Client calls server, with and without a return value.
// -------------------------------------------------------------------------------------------
Section("Client to server");
int total = await alice.CallAsync<int>("sum", 5, 7);
Console.WriteLine($"  alice   -> sum(5, 7) = {total}");

var echoed = await alice.CallAsync<string>("echo", "hello server");
Console.WriteLine($"  alice   -> echo returned \"{echoed}\"");

await bob.SendAsync("echo", "no reply wanted");
Console.WriteLine("  bob     -> echo sent fire-and-forget");

// -------------------------------------------------------------------------------------------
// 4. Errors come back as exceptions instead of hanging the caller.
// -------------------------------------------------------------------------------------------
Section("Failure travels");
try
{
    await alice.CallAsync<string>("explode", "now");
}
catch (SignalInvocationException ex)
{
    Console.WriteLine($"  alice   -> caught: {ex.RemoteMessage}");
}

try
{
    await alice.CallAsync<int>("no.such.method");
}
catch (MethodNotFoundException ex)
{
    Console.WriteLine($"  alice   -> caught: {ex.Message}");
}

// -------------------------------------------------------------------------------------------
// 5. Server to client: broadcast, direct, group, and a call that returns.
// -------------------------------------------------------------------------------------------
Section("Server to client");
await server.BroadcastAsync("serverHello", "all hands");
await Task.Delay(100);

await server.SendToClientAsync(alice.ClientId!, "serverHello", "just for alice");
await Task.Delay(100);

await alice.CallAsync<bool>("join", "operators");
await server.SendToGroupAsync("operators", "serverHello", "operators only");
await Task.Delay(100);

// v1 could not do this: the machinery was internal, so a server-to-client call never returned.
int doubled = await server.CallClientAsync<int>(bob.ClientId!, "double", 21);
Console.WriteLine($"  server  -> bob.double(21) = {doubled}");

// -------------------------------------------------------------------------------------------
// 6. A quick round-trip measurement, so the demo says something about speed too.
// -------------------------------------------------------------------------------------------
Section("Round trips");
const int iterations = 5_000;
for (int i = 0; i < 200; i++) await alice.CallAsync<int, int>("sum", i);   // warm up

long before = GC.GetTotalAllocatedBytes(precise: true);
var clock = Stopwatch.StartNew();
for (int i = 0; i < iterations; i++)
    await alice.CallAsync<int>("sum", i, 1);
clock.Stop();
long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

Console.WriteLine($"  {iterations:N0} calls in {clock.ElapsedMilliseconds} ms");
Console.WriteLine($"  {iterations / clock.Elapsed.TotalSeconds:N0} calls/sec, " +
                  $"{clock.Elapsed.TotalMicroseconds / iterations:0.0} us/call, " +
                  $"{(double)allocated / iterations:0} B/call");
Console.WriteLine();
Console.WriteLine($"  server  {server.Statistics}");
Console.WriteLine($"  alice   {alice.Statistics}");

Console.WriteLine();
Console.WriteLine("Press ENTER to stop.");
Console.ReadLine();
await cts.CancelAsync();

static string Short(string id) => id.Length > 8 ? id[..8] : id;

static void Section(string title)
{
    Console.WriteLine();
    Console.WriteLine($"-- {title} " + new string('-', Math.Max(0, 60 - title.Length)));
}

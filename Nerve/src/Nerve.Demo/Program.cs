// Nerve - built by Gravicode Studios, led by Kang Fadhil.
//
// Every feature of the library, end to end, in the order you would meet them. Run it with
//   dotnet run --project src/Nerve.Demo
using System.Diagnostics;
using Nerve;

Console.OutputEncoding = System.Text.Encoding.UTF8;
Line();
Console.WriteLine("  NERVE - in-process messaging for .NET 10");
Console.WriteLine("  Gravicode Studios, led by Kang Fadhil");
Line();

using var nerve = new NerveHub();
nerve.HandlerError += error =>
    Console.WriteLine($"    ! {error.SubscriptionFilter} failed on {error.Topic}: {error.Exception.Message}");

await PublishSubscribe();
await Wildcards();
await Retained();
await RequestReply();
await Streaming();
await ErrorIsolation();
Throughput();

Console.WriteLine();
Line();
Console.WriteLine($"  {nerve.GetStatistics()}");
Line();

// ============================== Publish/subscribe ==============================

async Task PublishSubscribe()
{
    Section("Publish and subscribe");

    using IDisposable reader = nerve.Subscribe<double>("sensor/tank-3/temperature",
        celsius => Console.WriteLine($"    reader     {celsius:N1} C"));

    using IDisposable alarm = nerve.Subscribe<double>("sensor/tank-3/temperature",
        celsius => celsius > 30,
        celsius => Console.WriteLine($"    alarm      over limit at {celsius:N1} C"));

    await nerve.PublishAsync("sensor/tank-3/temperature", 24.5);
    await nerve.PublishAsync("sensor/tank-3/temperature", 35.2);

    // Handlers run on the publishing thread, so both lines above are already printed.
    Console.WriteLine("    both handlers finished before PublishAsync returned");
}

// ================================== Wildcards ==================================

async Task Wildcards()
{
    Section("Wildcards: + is one level, # is the rest");

    using IDisposable perTank = nerve.Subscribe<double>("sensor/+/temperature",
        c => Console.WriteLine($"    sensor/+/temperature   {c:N1} C"));

    using IDisposable everything = nerve.Subscribe<double>("sensor/#",
        c => Console.WriteLine($"    sensor/#               {c:N1}"));

    await nerve.PublishAsync("sensor/tank-9/temperature", 28.1);
    await nerve.PublishAsync("sensor/tank-9/pressure", 1.4);
}

// ============================== Retained messages ==============================

async Task Retained()
{
    Section("Retained: the last value waits for whoever subscribes next");

    await nerve.PublishRetainedAsync("config/mode", "maintenance");
    Console.WriteLine("    published config/mode before anyone was listening");

    using IDisposable late = nerve.Subscribe<string>("config/mode",
        mode => Console.WriteLine($"    a late subscriber immediately received: {mode}"));
}

// =============================== Request/reply ================================

async Task RequestReply()
{
    Section("Request and reply, over the same topics");

    using IDisposable responder = nerve.Respond<string, int>("text/length", text => text.Length);
    Console.WriteLine($"    length of 'gravicode' = {await nerve.RequestAsync<string, int>("text/length", "gravicode")}");

    using IDisposable slow = nerve.Respond<int, string>("agents/+/ping", async (id, token) =>
    {
        await Task.Delay(20, token);
        return $"agent {id} is awake";
    });
    Console.WriteLine($"    {await nerve.RequestAsync<int, string>("agents/writer/ping", 4)}");

    try
    {
        await nerve.RequestAsync<int, int>("nobody/home", 1);
    }
    catch (NerveNoResponderException ex)
    {
        Console.WriteLine($"    a missing responder is reported at once, not after a timeout:");
        Console.WriteLine($"      {ex.Message}");
    }
}

// ================================== Streaming ==================================

async Task Streaming()
{
    Section("Streaming: a consumer with its own loop and a buffer");

    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    var drained = new List<int>();

    Task consumer = Task.Run(async () =>
    {
        await foreach (int tick in nerve.StreamAsync<int>("ticks", capacity: 16, cancellationToken: stop.Token))
        {
            drained.Add(tick);
            if (drained.Count == 5) await stop.CancelAsync();
        }
    });

    while (!nerve.HasSubscribers<int>("ticks")) await Task.Delay(5);
    for (int i = 1; i <= 5; i++) await nerve.PublishAsync("ticks", i);

    try { await consumer; } catch (OperationCanceledException) { }
    Console.WriteLine($"    the consumer drained {string.Join(", ", drained)} on its own thread");

    string ready = await WaitForExample();
    Console.WriteLine($"    WaitForAsync returned: {ready}");
}

async Task<string> WaitForExample()
{
    Task<string> waiter = nerve.WaitForAsync<string>("startup/ready", timeout: TimeSpan.FromSeconds(2));
    while (!nerve.HasSubscribers<string>("startup/ready")) await Task.Delay(5);
    await nerve.PublishAsync("startup/ready", "all agents reporting");
    return await waiter;
}

// =============================== Error isolation ===============================

async Task ErrorIsolation()
{
    Section("A broken subscriber cannot silence the others");

    using IDisposable broken = nerve.Subscribe<int>("orders/new", _ => throw new InvalidOperationException("database is down"));
    using IDisposable working = nerve.Subscribe<int>("orders/new", id => Console.WriteLine($"    order {id} still reached the second handler"));

    await nerve.PublishAsync("orders/new", 1041);
}

// ================================= Throughput ==================================

void Throughput()
{
    Section("Throughput on this machine");

    const int messages = 2_000_000;
    using var hub = new NerveHub(new NerveOptions { CollectStatistics = false });
    long received = 0;
    using IDisposable _ = hub.Subscribe<int>("bench", _ => received++);
    NerveTopic<int> topic = hub.Topic<int>("bench");

    for (int i = 0; i < messages; i++) topic.Publish(i);   // warm-up
    received = 0;

    GC.Collect();
    long before = GC.GetTotalAllocatedBytes(precise: true);
    var stopwatch = Stopwatch.StartNew();
    for (int i = 0; i < messages; i++) topic.Publish(i);
    stopwatch.Stop();
    long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

    Console.WriteLine($"    {messages:N0} messages in {stopwatch.ElapsedMilliseconds} ms");
    Console.WriteLine($"    {messages / stopwatch.Elapsed.TotalSeconds:N0} messages/second");
    Console.WriteLine($"    {allocated} bytes allocated across the whole run");
    Console.WriteLine($"    {received:N0} received");
    Console.WriteLine();
    Console.WriteLine("    For the full picture: dotnet run --project src/Nerve.Benchmarks -c Release -- --quick");
}

// =================================== Output ===================================

void Section(string title)
{
    Console.WriteLine();
    Console.WriteLine($"  {title}");
    Console.WriteLine($"  {new string('-', title.Length)}");
}

void Line() => Console.WriteLine(new string('=', 68));

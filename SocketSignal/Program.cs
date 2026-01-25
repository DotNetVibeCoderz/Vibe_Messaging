using SocketSignal;
using System.Text.Json;

// Sample server + client demo
var cts = new CancellationTokenSource();

var server = new SocketSignalServer("http://localhost:8080/ws/");

// Register server-side methods that clients can call
server.Register("echo", async (client, args) =>
{
    var text = args.Length > 0 ? args[0].GetString() : "";
    Console.WriteLine($"[Server] echo from {client.Id}: {text}");
    return $"echo:{text}";
});

server.Register("sum", async (client, args) =>
{
    var a = args[0].GetInt32();
    var b = args[1].GetInt32();
    return a + b;
});

_ = server.StartAsync(cts.Token);
Console.WriteLine("Server started at ws://localhost:8080/ws/");

// Demo client (C#)
var client = new SocketSignalClient();

client.On("serverHello", async (args) =>
{
    var message = args[0].GetString();
    Console.WriteLine($"[Client] serverHello: {message}");
    return "client received";
});

await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

// Call server method and get return value
var result = await client.CallAsync("sum", 5, 7);
Console.WriteLine($"[Client] sum result = {result?.GetInt32()}");

// Call server method without expecting return
await client.SendAsync("echo", "hello server");

// Server broadcasts to all clients
await server.BroadcastAsync("serverHello", "hello all clients");

Console.WriteLine("Press ENTER to stop...");
Console.ReadLine();
cts.Cancel();

# SocketSignal

**SocketSignal** adalah library komunikasi realtime dua arah (mirip Socket.IO) berbasis WebSocket murni di .NET. Library ini memungkinkan client dan server saling memanggil method secara realtime, baik dengan atau tanpa return value, serta mendukung broadcast, direct message, dan group message.

---

## ✅ Fitur Utama

1. Client dapat memanggil method di server.
2. Client dapat memanggil method di server dan mendapatkan return value.
3. Server dapat memanggil method ke semua client (broadcast).
4. Server dapat memanggil method ke client tertentu (by id).
5. Server dapat memanggil method ke grup client (by group name).

---

# 🇺🇸 English Documentation

## Quick Start (C# Client)

```csharp
var server = new SocketSignalServer("http://localhost:8080/ws/");

server.Register("sum", async (client, args) =>
{
    return args[0].GetInt32() + args[1].GetInt32();
});

_ = server.StartAsync();

var client = new SocketSignalClient();
await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

var result = await client.CallAsync("sum", 5, 7);
Console.WriteLine(result?.GetInt32());
```

### HTML + JavaScript Client

```html
<script>
const ws = new WebSocket("ws://localhost:8080/ws/");

ws.onmessage = async (ev) => {
    const msg = JSON.parse(ev.data);
    if(msg.type === "invoke" && msg.method === "serverHello") {
        // respond with return value
        const response = {
            type: "result",
            id: msg.id,
            result: "hello from browser"
        };
        ws.send(JSON.stringify(response));
    }
};

function callServerSum(a,b){
    const callId = crypto.randomUUID();
    const msg = {
        type: "invoke",
        id: callId,
        method: "sum",
        args: [a,b],
        expectReturn: true
    };
    ws.send(JSON.stringify(msg));
}
</script>
```

### Benchmark (Client-Server)

```csharp
await SocketSignal.Benchmark.BenchmarkClient.RunAsync();
```

---

# 🇮🇩 Dokumentasi Bahasa Indonesia

## Cara Pakai (Client C#)

```csharp
var server = new SocketSignalServer("http://localhost:8080/ws/");

server.Register("sum", async (client, args) =>
{
    return args[0].GetInt32() + args[1].GetInt32();
});

_ = server.StartAsync();

var client = new SocketSignalClient();
await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

var result = await client.CallAsync("sum", 5, 7);
Console.WriteLine(result?.GetInt32());
```

### Client HTML + JavaScript

```html
<script>
const ws = new WebSocket("ws://localhost:8080/ws/");

ws.onmessage = async (ev) => {
    const msg = JSON.parse(ev.data);
    if(msg.type === "invoke" && msg.method === "serverHello") {
        // balas return value
        const response = {
            type: "result",
            id: msg.id,
            result: "halo dari browser"
        };
        ws.send(JSON.stringify(response));
    }
};

function callServerSum(a,b){
    const callId = crypto.randomUUID();
    const msg = {
        type: "invoke",
        id: callId,
        method: "sum",
        args: [a,b],
        expectReturn: true
    };
    ws.send(JSON.stringify(msg));
}
</script>
```

### Benchmark (Client-Server)

```csharp
await SocketSignal.Benchmark.BenchmarkClient.RunAsync();
```

---

> Dibuat oleh tim di Gravicode Studios yang dipimpin oleh Kang Fadhil.

Kalau project ini membantu, boleh traktir pulsa ke saya di:
https://studios.gravicode.com/products/budax

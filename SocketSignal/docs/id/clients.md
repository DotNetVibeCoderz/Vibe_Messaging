# SDK client

*Gravicode Studios, dipimpin oleh Kang Fadhil.*

SocketSignal adalah JSON di atas WebSocket biasa, jadi apa pun yang bisa bicara WebSocket dapat
ikut serta. Tiga SDK disertakan di [`clients/`](../../clients), masing-masing sekitar 300 baris dan
berbentuk sama seperti client .NET: `on` untuk mendaftar, `call` untuk memanggil dan menunggu,
`send` untuk kirim-dan-lupakan.

Ketiganya diuji terhadap server .NET sungguhan di CI — gunanya punya tiga library client adalah
memastikan semuanya bicara protokol yang sama.

Jalankan sebuah server untuk contoh-contoh di bawah:

```bash
dotnet run --project src/SocketSignal.Demo -- serve
```

Perintah itu mendaftarkan `sum`, `echo`, `join`, dan `explode` di `ws://localhost:8080/ws/` lalu
terus berjalan.

---

## Python

Membutuhkan Python 3.10+ dan paket `websockets`.

```bash
pip install socketsignal

# atau jalankan contohnya dari hasil clone
cd clients/python
pip install websockets
python example.py
```

```python
import asyncio
from socketsignal import SocketSignalClient, SignalInvocationError

async def main():
    client = SocketSignalClient(call_timeout=10.0, keep_alive=15.0)

    # Method yang boleh dipanggil server pada kita. Sync atau async, keduanya boleh.
    @client.on("serverHello")
    async def hello(text):
        print("server bilang", text)
        return "python heard you"

    await client.connect("ws://localhost:8080/ws/")
    print(client.client_id)

    print(await client.call("sum", 5, 7))       # 12
    await client.send("log", "tanpa balasan")

    try:
        await client.call("explode", "now")
    except SignalInvocationError as error:
        print(error.remote_message)

    await client.close()

asyncio.run(main())
```

| | |
|---|---|
| Mendaftar | `client.on("nama", handler)` atau `@client.on("nama")` |
| Memanggil | `await client.call("nama", *args)` |
| Kirim-dan-lupakan | `await client.send("nama", *args)` |
| Error | `SignalInvocationError`, `SignalTimeoutError`, `SignalClosedError` |
| Sambung ulang | `SocketSignalClient(auto_reconnect=True)` |
| Context manager | `async with SocketSignalClient() as client:` |

---

## Node.js

Tanpa dependensi: Node 22 sudah menyediakan `WebSocket` global.

```bash
npm install @gravicode/socketsignal

# atau jalankan contohnya dari hasil clone
cd clients/nodejs
node example.mjs
```

```javascript
import { SocketSignalClient, SignalInvocationError } from "@gravicode/socketsignal";

const client = new SocketSignalClient({ callTimeoutMs: 10_000 });

client.on("serverHello", (text) => {
  console.log("server bilang", text);
  return "node heard you";
});

client.addEventListener("disconnected", (e) => console.log("terputus:", e.detail));

await client.connect("ws://localhost:8080/ws/");

console.log(await client.call("sum", 5, 7));   // 12
client.send("log", "tanpa balasan");

try {
  await client.call("explode", "now");
} catch (error) {
  console.log(error.remoteMessage);
}

client.close();
```

| | |
|---|---|
| Mendaftar | `client.on("nama", handler)` — handler async ikut ditunggu |
| Memanggil | `await client.call("nama", ...args)` |
| Kirim-dan-lupakan | `client.send("nama", ...args)` |
| Error | `SignalInvocationError`, `SignalTimeoutError`, `SignalClosedError` |
| Event | `connected`, `disconnected`, `reconnecting` lewat `addEventListener` |
| Sambung ulang | `new SocketSignalClient({ autoReconnect: true })` |

---

## Go

Membutuhkan Go 1.22+ dan `github.com/coder/websocket`.

```bash
go get github.com/DotNetVibeCoderz/Vibe_Messaging/SocketSignal/clients/go/v2/socketsignal

# atau jalankan contohnya dari hasil clone
cd clients/go
go mod tidy
go run ./example
```

```go
client := socketsignal.New(socketsignal.Options{
    CallTimeout: 10 * time.Second,
    OnDisconnected: func(reason string) { log.Println("terputus:", reason) },
})

// Argumen tiba sebagai JSON mentah, jadi handler hanya men-decode yang dibutuhkannya.
client.On("serverHello", func(args []json.RawMessage) (any, error) {
    var text string
    _ = json.Unmarshal(args[0], &text)
    return "go heard " + text, nil
})

if err := client.Connect(ctx, "ws://localhost:8080/ws/"); err != nil {
    log.Fatal(err)
}
defer client.Close()

var total int
if err := client.Call(ctx, &total, "sum", 5, 7); err != nil {
    log.Fatal(err)
}

_ = client.Send(ctx, "log", "tanpa balasan")
```

| | |
|---|---|
| Mendaftar | `client.On("nama", func(args []json.RawMessage) (any, error))` |
| Memanggil | `client.Call(ctx, &hasil, "nama", args...)` — kirim `nil` untuk mengabaikan hasil |
| Kirim-dan-lupakan | `client.Send(ctx, "nama", args...)` |
| Error | `*InvocationError`, `*TimeoutError`, `ErrClosed` |

Mengembalikan `error` bukan nil dari handler akan mengirim pesan itu kembali ke pemanggil.

---

## Browser

Tanpa SDK. Protokolnya cukup ringkas untuk ditulis langsung:

```html
<script>
const ws = new WebSocket("ws://localhost:8080/ws/");
const pending = new Map();
let nextId = 0;

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);

  if (msg.type === "welcome") {
    console.log("terhubung sebagai", msg.id);

  } else if (msg.type === "result") {
    const call = pending.get(msg.id);
    if (!call) return;
    pending.delete(msg.id);
    msg.error ? call.reject(new Error(msg.error)) : call.resolve(msg.result);

  } else if (msg.type === "invoke") {
    // Method yang dipanggil server pada kita.
    const result = msg.method === "serverHello" ? "halo dari browser" : null;
    if (msg.expectReturn) ws.send(JSON.stringify({ type: "result", id: msg.id, result }));

  } else if (msg.type === "ping") {
    ws.send(JSON.stringify({ type: "pong", id: msg.id }));
  }
};

function call(method, ...args) {
  const id = String(++nextId);
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ type: "invoke", id, method, args, expectReturn: true }));
  });
}

// await call("sum", 5, 7)  ->  12
</script>
```

Satu hal yang wajib benar adalah membalas `ping` dengan `pong`: tanpa itu, idle timeout server
cepat atau lambat akan memutus browser yang hanya mendengarkan.

---

## Menulis SDK untuk bahasa lain

Empat hal, dan client Anda sudah lengkap:

1. Buka WebSocket, baca `welcome`, simpan id-nya.
2. Simpan peta `id -> panggilan tertunda`. Saat `result` tiba, selesaikan dengan `result` atau
   gagalkan dengan `error`.
3. Saat `invoke` tiba, cari handler lokal. **Jika `expectReturn` diset, selalu balas** — dengan
   `result`, atau dengan `error` ketika handler melempar exception atau tidak ada.
4. Balas `ping` dengan `pong`.

Id hanya perlu unik dalam koneksi dan arah Anda sendiri; sebuah penghitung sudah cukup. Format
wire selengkapnya ada di [protocol.md](protocol.md), dan ketiga SDK itu masing-masing merupakan
acuan yang berjalan.

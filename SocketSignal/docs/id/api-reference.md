# Referensi API

*Gravicode Studios, dipimpin oleh Kang Fadhil.*

Namespace: `SocketSignal`.

---

## `SocketSignalServer`

Menerima client WebSocket dan meneruskan panggilan di antara mereka. `IAsyncDisposable`.

### Konstruksi

```csharp
new SocketSignalServer(string urlPrefix, SocketSignalOptions? options = null)
```

`urlPrefix` adalah prefiks `HttpListener` — `http://`, sementara client menghubungi `ws://`.

### Properti

| Anggota | Tipe | Catatan |
|---|---|---|
| `UrlPrefix` | `string` | Alamat yang didengarkan server |
| `Name` | `string` | Diumumkan pada frame welcome. Bawaan `"SocketSignal"` |
| `ClientCount` | `int` | Koneksi aktif. Murah |
| `Clients` | `IReadOnlyCollection<ClientConnection>` | Snapshot. Mengalokasi — pakai `ClientCount` di dalam loop |
| `GroupNames` | `IReadOnlyCollection<string>` | Grup yang punya minimal satu anggota |
| `Methods` | `IReadOnlyCollection<string>` | Nama method yang terdaftar |
| `Statistics` | `SignalStatistics` | Dihitung saat dibaca, mencakup koneksi aktif dan yang sudah tutup |
| `Authenticate` | `Func<HttpListenerContext, ValueTask<bool>>?` | Kembalikan false untuk menolak upgrade dengan 403 |

### Event

| Event | Tanda tangan |
|---|---|
| `ClientConnected` | `Action<ClientConnection>` |
| `ClientDisconnected` | `Action<ClientConnection, string>` — string-nya adalah alasan |

### Pendaftaran

```csharp
void Register(string method, Func<ClientConnection, JsonElement[], Task<object?>> handler)
void Register<TResult>(string method, Func<ClientConnection, ValueTask<TResult>> handler)
void Register<T1, TResult>(string method, Func<ClientConnection, T1?, ValueTask<TResult>> handler)
void Register<T1, T2, TResult>(string method, Func<ClientConnection, T1?, T2?, ValueTask<TResult>> handler)
void Register<T1, T2, T3, TResult>(string method, Func<ClientConnection, T1?, T2?, T3?, ValueTask<TResult>> handler)
bool Unregister(string method)
```

Mendaftarkan nama yang sama dua kali akan menggantikan handler-nya. Pendaftaran diharapkan
dilakukan saat awal: ia mengambil lock dan membangun ulang tabel dispatch, sementara pencarian di
jalur penerimaan bebas lock.

Argumen yang tidak dikirim pemanggil akan tiba sebagai `default`.

### Siklus hidup

```csharp
Task StartAsync(CancellationToken cancellationToken = default)
Task StopAsync()
ValueTask DisposeAsync()
```

`StartAsync` menjalankan loop penerimaan dan tidak kembali selama masih mendengarkan.

### Pengiriman

```csharp
Task BroadcastAsync(string method, params object?[] args)
Task SendToClientAsync(string clientId, string method, params object?[] args)
Task SendToGroupAsync(string groupName, string method, params object?[] args)
ValueTask<TResult?> CallClientAsync<TResult>(string clientId, string method, params object?[] args)
```

Tiga method fan-out bersifat kirim-dan-lupakan, dan satu client yang mati tidak bisa menggagalkan
broadcast ke client yang sehat. `CallClientAsync` melempar `SocketSignalException` bila client
tersebut tidak terhubung.

### Grup

```csharp
void AddToGroup(string groupName, string clientId)
void RemoveFromGroup(string groupName, string clientId)
IReadOnlyCollection<string> GroupMembers(string groupName)
IReadOnlyCollection<string> GroupsOf(string clientId)
```

Grup dibuat saat pertama dipakai dan keanggotaan dilepas ketika client terputus.

---

## `SocketSignalClient`

Terhubung ke server dan bertukar panggilan dengannya. `IAsyncDisposable`.

### Konstruksi

```csharp
new SocketSignalClient(SocketSignalOptions? options = null)
```

### Properti

| Anggota | Tipe | Catatan |
|---|---|---|
| `ClientId` | `string?` | Dari frame welcome. Null sebelum terhubung |
| `IsConnected` | `bool` | |
| `Statistics` | `SignalStatistics` | Untuk koneksi saat ini; menyambung ulang mereset-nya |
| `AutoReconnect` | `bool` | Bawaan false |
| `ReconnectDelay` | `TimeSpan` | Langkah backoff pertama. Bawaan 1 detik, berlipat dua |
| `MaxReconnectDelay` | `TimeSpan` | Batas atas backoff. Bawaan 30 detik |

### Event

| Event | Tanda tangan |
|---|---|
| `Connected` | `Action<string>` — id client yang diberikan |
| `Disconnected` | `Action<string>` — alasannya |
| `Reconnecting` | `Action<int>` — nomor percobaan |

### Pendaftaran

```csharp
void On(string method, Func<JsonElement[], Task<object?>> handler)
void On<TResult>(string method, Func<ValueTask<TResult>> handler)
void On<T1, TResult>(string method, Func<T1?, ValueTask<TResult>> handler)
void On<T1, T2, TResult>(string method, Func<T1?, T2?, ValueTask<TResult>> handler)
void On<T1, T2, T3, TResult>(string method, Func<T1?, T2?, T3?, ValueTask<TResult>> handler)
bool Off(string method)
```

### Koneksi

```csharp
Task ConnectAsync(Uri serverUri, CancellationToken cancellationToken = default)
Task DisconnectAsync()
ValueTask DisposeAsync()
```

`ConnectAsync` selesai ketika frame welcome tiba. `DisconnectAsync` menutup socket tetapi client
tetap bisa dipakai lagi; method ini juga mematikan `AutoReconnect`.

### Pemanggilan

```csharp
ValueTask<TResult?> CallAsync<TResult>(string method, params object?[] args)
ValueTask<TResult?> CallAsync<TArg, TResult>(string method, TArg arg)
ValueTask<JsonElement?> CallAsync(string method, params object?[] args)
ValueTask SendAsync(string method, params object?[] args)
ValueTask SendAsync<TArg>(string method, TArg arg)
```

Overload dengan dua parameter tipe menerima tepat satu argumen dan tidak pernah membuat `object[]`
maupun melakukan boxing pada tipe nilai. Gunakan itu di jalur padat.

---

## `ClientConnection`

Satu client yang terhubung, dari sudut pandang server. Handler menerimanya sebagai parameter
pertama.

| Anggota | Tipe | Catatan |
|---|---|---|
| `Id` | `string` | Id koneksi yang diberikan server |
| `RemoteEndPoint` | `IPEndPoint?` | Asal koneksi client, bila diketahui |
| `ConnectedAtUtc` | `DateTime` | |
| `Items` | `ConcurrentDictionary<string, object?>` | State aplikasi per koneksi |
| `Statistics` | `SignalStatistics` | Khusus koneksi ini |
| `IsOpen` | `bool` | |
| `Groups` | `IReadOnlyCollection<string>` | |

```csharp
ValueTask SendAsync(string method, params object?[] args)
ValueTask SendAsync<TArg>(string method, TArg arg)
ValueTask<TResult?> CallAsync<TResult>(string method, params object?[] args)
ValueTask<JsonElement?> CallAsync(string method, params object?[] args)
void JoinGroup(string groupName)
void LeaveGroup(string groupName)
ValueTask CloseAsync(string reason = "closed by server")
```

---

## `SocketSignalOptions`

| Properti | Bawaan | Arti |
|---|---|---|
| `CallTimeout` | 30 detik | Lama sebuah panggilan menunggu balasan. `Timeout.InfiniteTimeSpan` menunggu selamanya |
| `KeepAliveInterval` | 15 detik | Interval ping protokol pada koneksi menganggur. Infinite menonaktifkan |
| `IdleTimeout` | 60 detik | Lama diam sebelum server memutus koneksi |
| `MaxMessageSize` | 4 MB | Frame terbesar yang diterima. Melebihinya menutup koneksi |
| `ReceiveBufferSize` | 4 KB | Ukuran awal buffer terkumpul. Tumbuh sesuai kebutuhan dan tetap besar |
| `MaxConcurrentInvocations` | 64 | Handler yang berjalan bersamaan per koneksi; katup backpressure |
| `JsonOptions` | `SocketSignalOptions.Default` | camelCase, null dihilangkan, default web |

`SocketSignalOptions.Default` adalah `JsonSerializerOptions` bersama. Ganti `JsonOptions` untuk
menambah converter; jangan mengubah instance bersamanya.

---

## `SignalStatistics`

Penghitung interlocked, aman dibaca kapan saja.

| Anggota | Arti |
|---|---|
| `FramesSent` / `FramesReceived` | Frame protokol utuh |
| `BytesSent` / `BytesReceived` | Byte payload UTF-8, di luar framing WebSocket |
| `CallsCompleted` | Panggilan yang dikirim peer ini dan kembali membawa hasil |
| `CallsFailed` | Panggilan yang error, timeout, atau kehilangan socket |

---

## Exception

Semuanya turunan `SocketSignalException`.

| Exception | Muncul ketika | Tambahan |
|---|---|---|
| `SignalInvocationException` | Handler di seberang melempar exception | `Method`, `RemoteMessage` |
| `MethodNotFoundException` | Peer tidak punya method tersebut | `Method` |
| `SignalTimeoutException` | Tidak ada balasan dalam `CallTimeout` | `Method`, `Timeout` |
| `SignalConnectionClosedException` | Socket putus, atau client tidak terhubung | |

---

## Tipe pendukung

Publik, tetapi jarang dipakai langsung.

- **`SocketSignal.Protocol.MessageType`** — `Welcome`, `Invoke`, `Result`, `Ping`, `Pong`, `Unknown`.
- **`SocketSignal.Protocol.SignalFrame`** — `ref struct` yang menjadi jendela ke frame yang
  diterima. `TryParse` men-decode satu frame tanpa alokasi; setiap anggotanya adalah potongan dari
  buffer penerimaan dan hanya valid sampai pesan berikutnya.
- **`SocketSignal.Buffers.PooledBufferWriter`** — `IBufferWriter<byte>` yang bisa tumbuh di atas
  `ArrayPool<byte>.Shared`. `Reset` memundurkan kursor tanpa melepas buffer, dan itulah yang
  membuat koneksi berumur panjang bebas alokasi.

## Keamanan thread

- `SocketSignalServer` dan `SocketSignalClient` aman dipakai bersamaan dari banyak thread.
- Pengiriman pada satu koneksi diserialisasi secara internal, sehingga `SendAsync` yang bersamaan
  tidak mungkin saling menyisip di socket.
- Handler pada satu koneksi bisa berjalan bersamaan, sampai `MaxConcurrentInvocations`. Bila
  handler menyentuh state bersama, lindungi sendiri — urutan antar-invokasi bersamaan tidak dijamin.
- Sebuah `SignalFrame` tidak boleh keluar dari pemanggilan yang menerimanya.

# Nerve - In-App Messaging Library ⚡

![License](https://img.shields.io/badge/license-MIT-blue.svg) ![Language](https://img.shields.io/badge/language-C%23-green) ![Author](https://img.shields.io/badge/author-Jacky%20The%20Code%20Bender-orange)

**[Bahasa Indonesia]**

## Apa itu Nerve?
**Nerve** adalah perpustakaan (library) perpesanan *in-memory* yang sangat ringan, cepat, dan sederhana untuk aplikasi .NET. Library ini mengimplementasikan pola **Publish-Subscribe (Pub/Sub)** yang mirip dengan cara kerja MQTT, namun berjalan secara internal di dalam memori aplikasi Anda. 

Proyek ini dibuat untuk memudahkan komunikasi antar komponen dalam kode Anda tanpa perlu dependensi eksternal yang berat.

Dibuat dengan ❤️ oleh **Jacky the Code Bender** dari **Gravicode Studios**.

## Fitur Utama
- **Ringan & Cepat**: Menggunakan `ConcurrentDictionary` dan pemrosesan asinkron untuk performa tinggi.
- **Strongly Typed**: Mendukung Generic `Subscribe<T>` sehingga tipe data aman.
- **Easy Unsubscribe**: Mengembalikan objek `IDisposable` saat subscribe, sehingga bisa menggunakan blok `using` atau `.Dispose()` untuk berhenti berlangganan.
- **Async & Fire-and-Forget**: Mendukung `PublishAsync` (tunggu hingga semua handler selesai) dan `Publish` (kirim dan lupakan).
- **Thread Safe**: Aman digunakan dalam lingkungan *multithreaded*.

## Cara Penggunaan

### 1. Inisialisasi
Buat instance dari `NerveHub`. Anda biasanya hanya butuh satu instance (Singleton) untuk seluruh aplikasi.
```csharp
var nerve = new NerveHub();
```

### 2. Subscribe ke Topik
Anda bisa mendengarkan pesan pada topik tertentu.
```csharp
// Subscribe ke topik "sensor/suhu" yang mengirim data double
var subscription = nerve.Subscribe<double>("sensor/suhu", async (suhu) => 
{
    Console.WriteLine($"Suhu diterima: {suhu}°C");
});

// Jangan lupa dispose jika sudah tidak digunakan
// subscription.Dispose();
```

### 3. Publish Pesan
Kirim pesan ke semua subscriber yang mendengarkan topik tersebut.
```csharp
// Kirim data suhu
await nerve.PublishAsync("sensor/suhu", 25.5);

// Atau cara fire-and-forget
nerve.Publish("chat/general", "Halo dunia!");
```

## Benchmark
Nerve dilengkapi dengan uji benchmark bawaan di `Program.cs`. 
Dalam pengujian internal, Nerve mampu memproses **juta-an pesan per detik** (tergantung spesifikasi mesin), menjadikannya solusi yang sangat efisien untuk event bus lokal.

---

**[English]**

## What is Nerve?
**Nerve** is a lightweight, fast, and simple in-memory messaging library for .NET applications. It implements the **Publish-Subscribe (Pub/Sub)** pattern, similar to how MQTT works, but runs entirely internally within your application's memory.

This project was created to facilitate communication between code components without the need for heavy external dependencies.

Created with ❤️ by **Jacky the Code Bender** from **Gravicode Studios**.

## Key Features
- **Lightweight & Fast**: Uses `ConcurrentDictionary` and asynchronous processing for high performance.
- **Strongly Typed**: Supports Generic `Subscribe<T>` ensuring type safety.
- **Easy Unsubscribe**: Returns an `IDisposable` object upon subscription, allowing usage of `using` blocks or `.Dispose()` to unsubscribe cleanly.
- **Async & Fire-and-Forget**: Supports `PublishAsync` (await execution) and `Publish` (fire and forget).
- **Thread Safe**: Safe to use in multithreaded environments.

## How to Use

### 1. Initialization
Create an instance of `NerveHub`. You typically only need one instance (Singleton) for the entire application.
```csharp
var nerve = new NerveHub();
```

### 2. Subscribe to a Topic
Listen for messages on a specific topic.
```csharp
// Subscribe to topic "sensor/temp" expecting double data
var subscription = nerve.Subscribe<double>("sensor/temp", async (temp) => 
{
    Console.WriteLine($"Temperature received: {temp}°C");
});

// Don't forget to dispose if no longer needed
// subscription.Dispose();
```

### 3. Publish Messages
Send messages to all subscribers listening to that topic.
```csharp
// Send temperature data
await nerve.PublishAsync("sensor/temp", 25.5);

// Or fire-and-forget style
nerve.Publish("chat/general", "Hello World!");
```

## Benchmark
Nerve comes with a built-in benchmark in `Program.cs`. 
In internal tests, Nerve is capable of processing **millions of messages per second** (depending on machine specs), making it a highly efficient solution for local event buses.

---

### Support / Dukungan
Jika Anda menyukai proyek ini, jangan lupa traktir pulsanya ya bos!  
*If you like this project, don't forget to treat me to some phone credit!*

🔗 [Gravicode Studios](https://studios.gravicode.com)

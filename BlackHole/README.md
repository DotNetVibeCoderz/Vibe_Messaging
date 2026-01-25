# BlackHole Messaging V2 🕳️

![Language](https://img.shields.io/badge/Language-C%23-blue) ![Platform](https://img.shields.io/badge/Platform-.NET%209.0-purple) ![License](https://img.shields.io/badge/License-MIT-green)

[Bahasa Indonesia](#bahasa-indonesia) | [English](#english)

---

<a name="bahasa-indonesia"></a>
## 🇮🇩 Bahasa Indonesia

### Penjelasan Proyek
**BlackHole** adalah sebuah library dan framework perpesanan jaringan (network messaging) berkinerja tinggi yang dibuat dengan bahasa C#. Proyek ini mengimplementasikan protokol biner kustom di atas TCP untuk memfasilitasi komunikasi data yang cepat dan efisien.

Versi 2 ini memperkenalkan dukungan untuk pola pengiriman lanjutan seperti **Streaming** dan **Batching**, melengkapi fitur RPC (Remote Procedure Call) dan Pub/Sub yang sudah ada. Nama "BlackHole" melambangkan kemampuannya untuk menyerap data dalam jumlah besar dengan cepat.

### Fitur Utama
1.  **Custom TCP Protocol**: Menggunakan protokol biner *length-prefixed* untuk meminimalkan overhead dan memaksimalkan kecepatan.
2.  **RPC (Remote Procedure Call)**: Memungkinkan pemanggilan fungsi jarak jauh dengan pola *Request-Response*.
3.  **Pub/Sub (Publish/Subscribe)**: Pola komunikasi berbasis topik. Klien dapat *subscribe* ke topik tertentu dan menerima pesan yang di-*publish* ke topik tersebut.
4.  **Streaming**: Mendukung pengiriman data besar (seperti file video atau log besar) dengan memecahnya menjadi potongan-potongan (*chunks*) kecil (default 4KB).
5.  **Batching**: Memungkinkan penggabungan banyak pesan kecil menjadi satu paket jaringan untuk mengurangi *syscalls* dan meningkatkan *throughput*.

### Struktur Proyek
- **src/Models.cs**: Definisi struktur pesan dasar (`BlackHoleMessage`) dan tipe pesan (`MessageType`).
- **src/TcpTransport.cs**: Implementasi layer transport TCP (`TcpClientTransport`, `TcpServerHost`) yang menangani serialisasi/deserialisasi biner.
- **src/Patterns.cs**: Logic tingkat tinggi untuk pola-pola komunikasi (RPC, PubSub, Stream, Batch).
- **Program.cs**: Contoh aplikasi (Console) yang mendemonstrasikan cara menggunakan server dan klien secara bersamaan.

### Cara Menjalankan
1.  Pastikan Anda telah menginstal **.NET 9.0 SDK** atau versi yang lebih baru.
2.  Buka terminal atau command prompt di folder proyek ini.
3.  Jalankan perintah berikut:
    ```bash
    dotnet run
    ```
4.  Program akan mensimulasikan Server dan Client dalam satu aplikasi console, menampilkan demo RPC, Streaming, dan Batching.

### Contoh Penggunaan (Code Snippet)
```csharp
// Setup Transport
var transport = new TcpClientTransport("127.0.0.1", 5000);
await transport.StartAsync(CancellationToken.None);

// Menggunakan RPC Client
var rpcClient = new RpcClient(transport);
var response = await rpcClient.CallAsync("echo", Encoding.UTF8.GetBytes("Halo BlackHole!"));
```

---

<a name="english"></a>
## 🇺🇸 English

### Project Description
**BlackHole** is a high-performance network messaging library and framework written in C#. It implements a custom binary protocol over TCP to facilitate fast and efficient data communication.

Version 2 introduces support for advanced delivery patterns such as **Streaming** and **Batching**, complementing the existing RPC (Remote Procedure Call) and Pub/Sub features. The name "BlackHole" symbolizes its ability to absorb large amounts of data rapidly.

### Key Features
1.  **Custom TCP Protocol**: Uses a *length-prefixed* binary protocol to minimize overhead and maximize speed.
2.  **RPC (Remote Procedure Call)**: Enables remote function calls using a standard *Request-Response* pattern.
3.  **Pub/Sub (Publish/Subscribe)**: Topic-based communication pattern. Clients can subscribe to specific topics and receive messages published to them.
4.  **Streaming**: Supports sending large data (like video files or massive logs) by breaking them down into small chunks (default 4KB).
5.  **Batching**: Allows combining multiple small messages into a single network packet to reduce syscalls and increase throughput.

### Project Structure
- **src/Models.cs**: Definitions for basic message structures (`BlackHoleMessage`) and message types (`MessageType`).
- **src/TcpTransport.cs**: Implementation of the TCP transport layer (`TcpClientTransport`, `TcpServerHost`) handling binary serialization/deserialization.
- **src/Patterns.cs**: High-level logic for communication patterns (RPC, PubSub, Stream, Batch).
- **Program.cs**: An example application (Console) demonstrating how to use the server and client components together.

### How to Run
1.  Ensure you have **.NET 9.0 SDK** or later installed.
2.  Open a terminal or command prompt in this project folder.
3.  Run the following command:
    ```bash
    dotnet run
    ```
4.  The program will simulate both a Server and a Client in a single console application, showcasing demos for RPC, Streaming, and Batching.

### Usage Example (Code Snippet)
```csharp
// Setup Transport
var transport = new TcpClientTransport("127.0.0.1", 5000);
await transport.StartAsync(CancellationToken.None);

// Using RPC Client
var rpcClient = new RpcClient(transport);
var response = await rpcClient.CallAsync("echo", Encoding.UTF8.GetBytes("Hello BlackHole!"));
```

---

### Author
Code written by **Jacky the code bender** & Gravicode Studios Team.
*Don't forget the phone credit treat! (Traktiran pulsa)* :p

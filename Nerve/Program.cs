using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Nerve
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=================================================");
            Console.WriteLine("   NERVE - In-App Messaging Library (Like MQTT)");
            Console.WriteLine("   Created by Jacky the Code Bender");
            Console.WriteLine("   (Gravicode Studios)");
            Console.WriteLine("=================================================");
            Console.WriteLine("");

            // Langsung jalankan Demo dan Benchmark untuk demonstrasi
            // (Menu interaktif saya disable agar bisa jalan otomatis di console sandbox/CI)

            Console.WriteLine(">>> STEP 1: MENJALANKAN DEMO <<<");
            await RunDemo();

            Console.WriteLine("\n-------------------------------------------");
            Console.WriteLine(">>> STEP 2: MENJALANKAN BENCHMARK <<<");
            Console.WriteLine("    (Mohon tunggu sebentar...)");
            await RunBenchmark();

            Console.WriteLine("\n=================================================");
            Console.WriteLine("   SEMUA SELESAI.");
            Console.WriteLine("   Jangan lupa traktir pulsanya ya bos!");
            Console.WriteLine("=================================================");
        }

        static async Task RunDemo()
        {
            Console.WriteLine("\n--- DEMO START ---");
            var nerve = new NerveHub();

            // 1. Subscribe ke topik "sensor/suhu"
            // Kita pakai 'using' agar otomatis unsubscribe saat scope habis
            using (var sub1 = nerve.Subscribe<double>("sensor/suhu", (suhu) => 
            {
                // Tambahkan delay simulasi processing 10ms
                // await Task.Delay(10); 
                Console.WriteLine($"[Subscriber 1] Menerima Suhu: {suhu}°C");
                return Task.CompletedTask;
            }))
            {
                // Subscriber kedua di topik yang sama
                var sub2 = nerve.Subscribe<double>("sensor/suhu", (suhu) => 
                {
                    if (suhu > 30)
                        Console.WriteLine($"[Subscriber 2] PERINGATAN! Suhu panas: {suhu}°C");
                });

                // Subscribe ke topik lain
                var chatSub = nerve.Subscribe<string>("chat/general", (msg) => 
                {
                    Console.WriteLine($"[Chat] User berkata: {msg}");
                    return Task.CompletedTask; 
                });

                Console.WriteLine("Subscriber siap. Mengirim pesan...\n");

                // 2. Publish pesan
                await nerve.PublishAsync("sensor/suhu", 24.5);
                await Task.Delay(100);
                
                await nerve.PublishAsync("sensor/suhu", 35.2); // Akan mentrigger warning
                await Task.Delay(100);

                await nerve.PublishAsync("chat/general", "Halo Jacky! Minta pulsa dong.");
                await Task.Delay(100);
                
                // Unsubscribe sub2 secara manual
                Console.WriteLine("Unsubscribing Subscriber 2...");
                sub2.Dispose(); 

                await nerve.PublishAsync("sensor/suhu", 40.0); // Sub 2 tidak akan bunyi
                await Task.Delay(100);
            } 
            
            Console.WriteLine("--- DEMO END ---");
        }

        static async Task RunBenchmark()
        {
            Console.WriteLine("\n--- BENCHMARK START ---");
            var nerve = new NerveHub();
            int messageCount = 1_000_000; // 1 Juta pesan
            int receivedCount = 0;
            
            var tcs = new TaskCompletionSource<bool>();

            // Subscribe
            var sub = nerve.Subscribe<int>("benchmark", (num) => 
            {
                // Interlocked agar thread-safe dan cepat
                var current = System.Threading.Interlocked.Increment(ref receivedCount);
                if (current == messageCount)
                {
                    tcs.SetResult(true);
                }
                return Task.CompletedTask;
            });

            Console.WriteLine($"Memulai pengiriman {messageCount:N0} pesan...");
            var sw = Stopwatch.StartNew();

            // Publish loop
            for (int i = 0; i < messageCount; i++)
            {
                // Menggunakan Fire-and-Forget Publish untuk simulasi load tinggi
                nerve.Publish("benchmark", i);
            }

            // Tunggu consumer selesai
            await tcs.Task;

            sw.Stop();
            Console.WriteLine($"Selesai!");
            Console.WriteLine($"Waktu: {sw.ElapsedMilliseconds} ms");
            
            if (sw.Elapsed.TotalSeconds > 0)
            {
                var throughput = messageCount / sw.Elapsed.TotalSeconds;
                Console.WriteLine($"Throughput: {throughput:N0} pesan/detik");
            }
            
            sub.Dispose();
            Console.WriteLine("--- BENCHMARK END ---");
        }
    }
}

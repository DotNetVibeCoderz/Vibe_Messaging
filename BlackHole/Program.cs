using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BlackHole.Common;
using BlackHole.Patterns;
using BlackHole.Transports;
using System.IO;
using System.Collections.Generic;

namespace BlackHole
{
    class Program
    {
        // Settings
        const int PORT = 5000;
        const string HOST = "127.0.0.1";

        static async Task Main(string[] args)
        {
            Console.WriteLine("==============================================");
            Console.WriteLine("   BLACK HOLE MESSAGING - V2 (Stream & Batch)  ");
            Console.WriteLine("==============================================");
            Console.WriteLine("Halo again! Jacky the code bender updated this.");
            Console.WriteLine("Traktiran pulsa still waiting... :p");
            Console.WriteLine("");

            // 1. Setup Server Elements
            var server = new TcpServerHost(PORT);
            var rpcServer = new RpcServer();
            var pubSubBroker = new PubSubBroker();
           
            // Register RPC
            rpcServer.RegisterMethod("echo", (payload) => payload);

            server.OnClientConnected += (s, transport) => 
            {
                // Attach logic to connected client
                // Note: Real world you'd have more sophisticated router or pipeline
                var streamRecv = new StreamReceiver(transport);
                streamRecv.OnStreamCompleted += (id, data) => 
                {
                    Console.WriteLine($"[Server] Stream '{id}' received fully ({data.Length} bytes).");
                };
                
                var batchRecv = new BatchReceiver(transport);
                batchRecv.OnMessageProcessed += (innerMsg) => 
                {
                    Console.WriteLine($"[Server] Batch Inner Msg: {innerMsg.Type} - {innerMsg.Header}");
                };

                transport.OnMessageReceived += (sender, msg) => 
                {
                    if (msg.Type == MessageType.RpcRequest)
                        rpcServer.HandlePacket((ITransport)sender, msg);
                    else if (msg.Type == MessageType.Subscribe || msg.Type == MessageType.Publish)
                        pubSubBroker.HandlePacket((ITransport)sender, msg);
                };
                Console.WriteLine("[Server] Client Connected!");
            };

            server.Start();
            Console.WriteLine($"[Server] Listening on {PORT}...");

            // 2. Setup Client
            await Task.Delay(1000); 
            var transport = new TcpClientTransport(HOST, PORT);
            await transport.StartAsync(CancellationToken.None);

            var rpcClient = new RpcClient(transport);
            var pubSubClient = new PubSubClient(transport);
            var streamSender = new StreamSender(transport);
            var batchSender = new BatchSender(transport);

            // 3. Test Streaming
            Console.WriteLine("\n--- Testing Streaming ---");
            // Simulate a large file/data
            var largeData = new byte[1024 * 100]; // 100KB
            using (var ms = new MemoryStream(largeData))
            {
                // Send in chunks of 4KB
                await streamSender.SendStreamAsync("video-upload-01", ms, 4096);
            }
            await Task.Delay(500);

            // 4. Test Batching
            Console.WriteLine("\n--- Testing Batching ---");
            var batchList = new List<BlackHoleMessage>();
            for(int i=0; i<5; i++)
            {
                batchList.Add(new BlackHoleMessage 
                {
                    Type = MessageType.Publish, // Example inner logic
                    Header = $"log/entry/{i}",
                    Payload = Encoding.UTF8.GetBytes($"Log Data {i}")
                });
            }
            await batchSender.SendBatchAsync(batchList);
            await Task.Delay(500);

            // 5. Benchmark RPC (Quick check)
            Console.WriteLine("\n--- Benchmarking RPC (5,000 requests) ---");
            var payloadData = Encoding.UTF8.GetBytes("Speed Check");
            await rpcClient.CallAsync("echo", payloadData); // Warmup

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 5000; i++)
            {
                await rpcClient.CallAsync("echo", payloadData);
            }
            sw.Stop();
            Console.WriteLine($"Total Time: {sw.Elapsed.TotalMilliseconds:F2} ms");

            Console.WriteLine("\n[Press Enter to Exit]");
            Console.ReadLine();
        }
    }
}
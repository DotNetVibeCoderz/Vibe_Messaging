using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using BlackHole.Common;
using System.IO;

namespace BlackHole.Patterns
{
    // --- RPC ---
    public class RpcServer
    {
        private readonly Dictionary<string, Func<byte[], byte[]>> _methods = new();

        public void RegisterMethod(string methodName, Func<byte[], byte[]> handler)
        {
            _methods[methodName] = handler;
        }

        public void HandlePacket(ITransport sender, BlackHoleMessage msg)
        {
            if (msg.Type == MessageType.RpcRequest)
            {
                if (_methods.TryGetValue(msg.Header, out var handler))
                {
                    // Execute
                    var result = handler(msg.Payload);
                    
                    // Respond
                    var response = new BlackHoleMessage
                    {
                        CheckId = msg.CheckId, // Correlate ID
                        Type = MessageType.RpcResponse,
                        Header = msg.Header,
                        Payload = result
                    };
                    sender.SendAsync(response);
                }
            }
        }
    }

    public class RpcClient
    {
        private readonly ITransport _transport;
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<byte[]>> _pendingRequests = new();

        public RpcClient(ITransport transport)
        {
            _transport = transport;
            _transport.OnMessageReceived += OnMessage;
        }

        private void OnMessage(object? sender, BlackHoleMessage msg)
        {
            if (msg.Type == MessageType.RpcResponse)
            {
                if (_pendingRequests.TryRemove(msg.CheckId, out var tcs))
                {
                    tcs.SetResult(msg.Payload);
                }
            }
        }

        public async Task<byte[]> CallAsync(string method, byte[] payload)
        {
            var msg = new BlackHoleMessage
            {
                Type = MessageType.RpcRequest,
                Header = method,
                Payload = payload
            };

            var tcs = new TaskCompletionSource<byte[]>();
            _pendingRequests[msg.CheckId] = tcs;

            await _transport.SendAsync(msg);
            return await tcs.Task;
        }
    }

    // --- PUB SUB ---
    public class PubSubBroker
    {
        private readonly ConcurrentDictionary<string, List<ITransport>> _subscribers = new();

        public void HandlePacket(ITransport sender, BlackHoleMessage msg)
        {
            if (msg.Type == MessageType.Subscribe)
            {
                // Header is Topic
                var topic = msg.Header;
                _subscribers.AddOrUpdate(topic, 
                    new List<ITransport> { sender }, 
                    (key, list) => {
                        lock(list) {
                            if (!list.Contains(sender)) list.Add(sender);
                        }
                        return list;
                    });
                Console.WriteLine($"[Broker] Client subscribed to {topic}");
            }
            else if (msg.Type == MessageType.Publish)
            {
                var topic = msg.Header;
                if (_subscribers.TryGetValue(topic, out var subs))
                {
                    lock(subs)
                    {
                        foreach(var sub in subs)
                        {
                            // Forward message
                            // Don't echo back to sender if you want, but normally pubsub echoes to all
                            sub.SendAsync(new BlackHoleMessage
                            {
                                Type = MessageType.Publish,
                                Header = topic,
                                Payload = msg.Payload
                            });
                        }
                    }
                }
            }
        }
    }

    public class PubSubClient
    {
        private readonly ITransport _transport;
        public event Action<string, byte[]>? OnTopicReceived;

        public PubSubClient(ITransport transport)
        {
            _transport = transport;
            _transport.OnMessageReceived += OnMessage;
        }

        private void OnMessage(object? sender, BlackHoleMessage msg)
        {
            if (msg.Type == MessageType.Publish)
            {
                OnTopicReceived?.Invoke(msg.Header, msg.Payload);
            }
        }

        public async Task SubscribeAsync(string topic)
        {
            await _transport.SendAsync(new BlackHoleMessage
            {
                Type = MessageType.Subscribe,
                Header = topic
            });
        }

        public async Task PublishAsync(string topic, byte[] data)
        {
            await _transport.SendAsync(new BlackHoleMessage
            {
                Type = MessageType.Publish,
                Header = topic,
                Payload = data
            });
        }
    }

    // --- STREAMING SUPPORT ---
    public class StreamReceiver
    {
       private readonly ITransport _transport;
       private readonly ConcurrentDictionary<string, MemoryStream> _incomingStreams = new();
       public event Action<string, byte[]>? OnStreamCompleted;
       public event Action<string, byte[]>? OnStreamProgress;

       public StreamReceiver(ITransport transport)
       {
           _transport = transport;
           _transport.OnMessageReceived += OnMessage;
       }

       private void OnMessage(object? sender, BlackHoleMessage msg)
       {
           if (msg.Type == MessageType.StreamStart) 
           {
               _incomingStreams[msg.Header] = new MemoryStream();
               Console.WriteLine($"[StreamReceiver] Started stream {msg.Header}");
           }
           else if (msg.Type == MessageType.StreamChunk)
           {
               if (_incomingStreams.TryGetValue(msg.Header, out var ms))
               {
                   ms.Write(msg.Payload, 0, msg.Payload.Length);
                   // Notify progress
                    OnStreamProgress?.Invoke(msg.Header, msg.Payload);
               }
           }
           else if (msg.Type == MessageType.StreamEnd)
           {
               if (_incomingStreams.TryRemove(msg.Header, out var ms))
               {
                   OnStreamCompleted?.Invoke(msg.Header, ms.ToArray());
                   ms.Dispose();
                   Console.WriteLine($"[StreamReceiver] Stream {msg.Header} completed.");
               }
           }
       }
    }

    public class StreamSender
    {
        private readonly ITransport _transport;

        public StreamSender(ITransport transport)
        {
            _transport = transport;
        }

        public async Task SendStreamAsync(string streamId, Stream dataStream, int chunkSize = 4096)
        {
            // Start
            await _transport.SendAsync(new BlackHoleMessage { Type = MessageType.StreamStart, Header = streamId });

            // Chunk
            var buffer = new byte[chunkSize];
            int read;
            while((read = await dataStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                var payload = new byte[read];
                Array.Copy(buffer, payload, read);
                await _transport.SendAsync(new BlackHoleMessage 
                { 
                    Type = MessageType.StreamChunk, 
                    Header = streamId,
                    Payload = payload
                });
            }

            // End
            await _transport.SendAsync(new BlackHoleMessage { Type = MessageType.StreamEnd, Header = streamId });
        }
    }

    // --- BATCH SUPPORT ---
    public class BatchSender
    {
        private readonly ITransport _transport;
        public event EventHandler? OnBatchProcessed; // Signal back

        public BatchSender(ITransport transport)
        {
            _transport = transport;
        }

        public async Task SendBatchAsync(IEnumerable<BlackHoleMessage> messages)
        {
             // Simple naive batching: Serialize all into one payload or send sequentially rapidly
             // High perf batching: combine multiple message bytes into ONE tcp packet
             // For demo, we'll encapsulate a list of messages into a single wrapper Type=Batch
             
             using (var ms = new MemoryStream())
             using (var bw = new BinaryWriter(ms))
             {
                 int count = 0;
                 foreach(var m in messages)
                 {
                     // Write each sub-message
                     // Format: [Type][HeaderLen][Header][PayloadLen][Payload]
                     bw.Write((byte)m.Type);
                     var h = Encoding.UTF8.GetBytes(m.Header ?? "");
                     bw.Write(h.Length);
                     bw.Write(h);
                     bw.Write(m.Payload.Length);
                     bw.Write(m.Payload);
                     count++;
                 }

                 var batchPayload = ms.ToArray();
                 
                 // Send as one big Batch Message
                await _transport.SendAsync(new BlackHoleMessage
                {
                    Type = MessageType.Batch,
                    Header = count.ToString(), // Store count in header
                    Payload = batchPayload
                });
             }
        }
    }

    public class BatchReceiver
    {
        private readonly ITransport _transport;
        // You can register handler per inner type or generic
        public event Action<BlackHoleMessage>? OnMessageProcessed;

        public BatchReceiver(ITransport transport)
        {
            _transport = transport;
            _transport.OnMessageReceived += OnMessage;
        }

        private void OnMessage(object? sender, BlackHoleMessage msg)
        {
            if (msg.Type == MessageType.Batch)
            {
                // Unpack
                ProcessBatch(msg.Payload);
            }
        }

        private void ProcessBatch(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var br = new BinaryReader(ms))
            {
                while(ms.Position < ms.Length)
                {
                    var type = (MessageType)br.ReadByte();
                    var hLen = br.ReadInt32();
                    var header = Encoding.UTF8.GetString(br.ReadBytes(hLen));
                    var pLen = br.ReadInt32();
                    var payload = br.ReadBytes(pLen);

                    var m = new BlackHoleMessage 
                    {
                        Type = type,
                        Header = header,
                        Payload = payload
                    };
                    OnMessageProcessed?.Invoke(m);
                }
            }
        }
    }
}
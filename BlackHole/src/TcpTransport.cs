using System;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BlackHole.Common;

namespace BlackHole.Transports
{
    // High performance TCP Transport handling Length-Prefixed messages
    public class TcpClientTransport : ITransport
    {
        private TcpClient _client;
        private NetworkStream _stream;
        private readonly string _ip;
        private readonly int _port;

        public event EventHandler<BlackHoleMessage>? OnMessageReceived;

        public TcpClientTransport(string ip, int port)
        {
            _ip = ip;
            _port = port;
            _client = new TcpClient();
        }

        public async Task StartAsync(CancellationToken ct)
        {
            if (!_client.Connected)
                await _client.ConnectAsync(_ip, _port);
            
            _stream = _client.GetStream();
            
            // Start receive loop
            _ = Task.Run(() => ReceiveLoop(ct), ct);
        }

        public async Task SendAsync(BlackHoleMessage message)
        {
            if (_client == null || !_client.Connected) throw new Exception("Not connected");

            var packet = Serialize(message);
            await _stream.WriteAsync(packet, 0, packet.Length);
        }

        private byte[] Serialize(BlackHoleMessage msg)
        {
            using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            {
                // Format: [TotalLen(4)][MsgId(16)][Type(1)][HeaderLen(4)][Header(bytes)][Payload(bytes)]
                
                // We'll write body first to calc length, then wrap
                using (var bodyMs = new MemoryStream())
                using (var bodyBw = new BinaryWriter(bodyMs))
                {
                    bodyBw.Write(msg.CheckId.ToByteArray());
                    bodyBw.Write((byte)msg.Type);
                    var headerBytes = Encoding.UTF8.GetBytes(msg.Header ?? "");
                    bodyBw.Write(headerBytes.Length);
                    bodyBw.Write(headerBytes);
                    bodyBw.Write(msg.Payload.Length);
                    bodyBw.Write(msg.Payload);

                    var body = bodyMs.ToArray();
                    bw.Write(body.Length); // Length prefix
                    bw.Write(body);
                }
                return ms.ToArray();
            }
        }

        private async Task ReceiveLoop(CancellationToken ct)
        {
            var lenBuffer = new byte[4];
            while (!ct.IsCancellationRequested && _client.Connected)
            {
                try
                {
                    // Read Length
                    int bytesRead = 0;
                    while (bytesRead < 4)
                    {
                        int r = await _stream.ReadAsync(lenBuffer, bytesRead, 4 - bytesRead, ct);
                        if (r == 0) return; // Disconnected
                        bytesRead += r;
                    }
                    int packageLen = BitConverter.ToInt32(lenBuffer, 0);

                    // Read Body
                    var bodyBuffer = new byte[packageLen];
                    bytesRead = 0;
                    while (bytesRead < packageLen)
                    {
                        int r = await _stream.ReadAsync(bodyBuffer, bytesRead, packageLen - bytesRead, ct);
                        if (r == 0) return;
                        bytesRead += r;
                    }

                    // Deserialize
                    var msg = Deserialize(bodyBuffer);
                    OnMessageReceived?.Invoke(this, msg);
                }
                catch (Exception)
                {
                    break;
                }
            }
        }

        private BlackHoleMessage Deserialize(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var br = new BinaryReader(ms))
            {
                var msg = new BlackHoleMessage();
                msg.CheckId = new Guid(br.ReadBytes(16));
                msg.Type = (MessageType)br.ReadByte();
                
                int headerLen = br.ReadInt32();
                msg.Header = Encoding.UTF8.GetString(br.ReadBytes(headerLen));
                
                int payloadLen = br.ReadInt32();
                msg.Payload = br.ReadBytes(payloadLen);
                return msg;
            }
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _client?.Close();
        }
    }

    // A Simple TCP Server for listening
    public class TcpServerHost
    {
        private TcpListener _listener;
        public event EventHandler<ITransport>? OnClientConnected;

        public TcpServerHost(int port)
        {
            _listener = new TcpListener(IPAddress.Any, port);
        }

        public void Start()
        {
            _listener.Start();
             _ = Task.Run(AcceptLoop);
        }

        private async Task AcceptLoop()
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync();
                
                // Wrap the TcpClient into our Transport interface logic
                // For simplified server side handling, we need a wrapper similar to TcpClientTransport
                // but utilizing the already connected client.
                var transport = new TcpServerSideTransport(client);
                transport.Start(); // Start listening
                OnClientConnected?.Invoke(this, transport);
            }
        }
    }

    public class TcpServerSideTransport : ITransport
    {
        private TcpClient _client;
        private NetworkStream _stream;
        public event EventHandler<BlackHoleMessage>? OnMessageReceived;

        public TcpServerSideTransport(TcpClient client)
        {
            _client = client;
            _stream = client.GetStream();
        }

        public void Start() { 
             _ = Task.Run(() => ReceiveLoop());
        }

        public Task StartAsync(CancellationToken ct) 
        {
            // Already connected on server side
            Start();
            return Task.CompletedTask;
        }

        private async Task ReceiveLoop()
        {
           // Same logic as client loop, simplified for brevity duplication
           // In real prod, this logic should be shared.
           var lenBuffer = new byte[4];
            while (_client.Connected)
            {
                try
                {
                    int bytesRead = 0;
                    while (bytesRead < 4)
                    {
                        int r = await _stream.ReadAsync(lenBuffer, bytesRead, 4 - bytesRead);
                        if (r == 0) return;
                        bytesRead += r;
                    }
                    int packageLen = BitConverter.ToInt32(lenBuffer, 0);

                    var bodyBuffer = new byte[packageLen];
                    bytesRead = 0;
                    while (bytesRead < packageLen)
                    {
                        int r = await _stream.ReadAsync(bodyBuffer, bytesRead, packageLen - bytesRead);
                        if (r == 0) return;
                        bytesRead += r;
                    }
                    
                    var msg = Deserialize(bodyBuffer);
                    OnMessageReceived?.Invoke(this, msg);
                }
                catch { break; }
            }
        }

        public async Task SendAsync(BlackHoleMessage message)
        {
            if (!_client.Connected) return;
            var packet = Serialize(message);
            await _stream.WriteAsync(packet, 0, packet.Length);
        }

        // Duplicated for simplicity in this single file, normally use a helper
        private byte[] Serialize(BlackHoleMessage msg)
        {
             using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            {
                using (var bodyMs = new MemoryStream())
                using (var bodyBw = new BinaryWriter(bodyMs))
                {
                    bodyBw.Write(msg.CheckId.ToByteArray());
                    bodyBw.Write((byte)msg.Type);
                    var headerBytes = Encoding.UTF8.GetBytes(msg.Header ?? "");
                    bodyBw.Write(headerBytes.Length);
                    bodyBw.Write(headerBytes);
                    bodyBw.Write(msg.Payload.Length);
                    bodyBw.Write(msg.Payload);

                    var body = bodyMs.ToArray();
                    bw.Write(body.Length); 
                    bw.Write(body);
                }
                return ms.ToArray();
            }
        }

        private BlackHoleMessage Deserialize(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var br = new BinaryReader(ms))
            {
                var msg = new BlackHoleMessage();
                msg.CheckId = new Guid(br.ReadBytes(16));
                msg.Type = (MessageType)br.ReadByte();
                int headerLen = br.ReadInt32();
                msg.Header = Encoding.UTF8.GetString(br.ReadBytes(headerLen));
                int payloadLen = br.ReadInt32();
                msg.Payload = br.ReadBytes(payloadLen);
                return msg;
            }
        }
        
        public void Dispose() { _client.Close(); }
    }
}
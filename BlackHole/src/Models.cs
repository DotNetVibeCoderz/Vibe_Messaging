using System;

namespace BlackHole.Common
{
    public enum MessageType : byte
    {
        RpcRequest = 0x01,
        RpcResponse = 0x02,
        Publish = 0x03,
        Subscribe = 0x04,
        Ack = 0x05,
        // Streaming
        StreamStart = 0x10,
        StreamChunk = 0x11,
        StreamEnd = 0x12,
        // Batch
        Batch = 0x20
    }

    public class BlackHoleMessage
    {
        public Guid CheckId { get; set; } = Guid.NewGuid();
        public MessageType Type { get; set; }
        public string Header { get; set; } = string.Empty; // Rpc Method Name, Topic, or StreamID
        public byte[] Payload { get; set; } = Array.Empty<byte>();
    }

    public interface ITransport : IDisposable
    {
        Task StartAsync(CancellationToken ct);
        Task SendAsync(BlackHoleMessage message);
        event EventHandler<BlackHoleMessage> OnMessageReceived;
    }
}
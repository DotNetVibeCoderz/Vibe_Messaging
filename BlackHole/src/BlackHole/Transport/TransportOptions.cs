// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using BlackHole.Protocol;

namespace BlackHole.Transport;

/// <summary>
/// Knobs shared by both ends of a connection. The defaults are tuned for many small messages, which
/// is what RPC, telemetry, and Pub/Sub traffic actually look like.
/// </summary>
public sealed class TransportOptions
{
    /// <summary>Reject any frame larger than this while parsing. Default 16 MiB.</summary>
    public int MaxFrameLength { get; set; } = FrameCodec.DefaultMaxFrameLength;

    /// <summary>Read buffer handed to the pipe. Default 64 KiB.</summary>
    public int ReceiveBufferSize { get; set; } = 64 * 1024;

    /// <summary>Minimum write buffer segment. Default 8 KiB.</summary>
    public int SendBufferSize { get; set; } = 8 * 1024;

    /// <summary>
    /// Disable Nagle. On by default: BlackHole coalesces at the application layer (WriteAsync plus
    /// FlushAsync, or BatchSender), so letting the kernel hold small frames only adds latency.
    /// </summary>
    public bool NoDelay { get; set; } = true;

    /// <summary>
    /// How often an idle connection sends a Ping. Null disables keepalive. Default 30s.
    /// </summary>
    public TimeSpan? KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Slots in the per-connection header cache. Default 512.</summary>
    public int HeaderCacheCapacity { get; set; } = 512;

    /// <summary>
    /// Optional shared header cache. Leave null for one cache per connection; supply a shared
    /// instance when thousands of connections use the same small set of topics.
    /// </summary>
    public HeaderCache? SharedHeaderCache { get; set; }

    /// <summary>Called when the receive loop dies from anything other than a clean close.</summary>
    public Action<Exception>? ErrorHandler { get; set; }

    internal HeaderCache CreateHeaderCache() => SharedHeaderCache ?? new HeaderCache(HeaderCacheCapacity);

    /// <summary>A copy, so a caller can hand the same template to several connections and then tweak it.</summary>
    public TransportOptions Clone() => (TransportOptions)MemberwiseClone();
}

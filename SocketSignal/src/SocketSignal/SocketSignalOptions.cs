// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SocketSignal;

/// <summary>Tuning shared by <see cref="SocketSignalServer"/> and <see cref="SocketSignalClient"/>.</summary>
public sealed class SocketSignalOptions
{
    /// <summary>How long <c>CallAsync</c> waits for a reply before failing. Default 30s.</summary>
    /// <remarks>Set to <see cref="Timeout.InfiniteTimeSpan"/> to wait forever.</remarks>
    public TimeSpan CallTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often a keepalive ping goes out on an idle connection. Default 15s.</summary>
    /// <remarks>Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable keepalive entirely.</remarks>
    public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Silence after which a connection is considered dead and torn down. Default 60s.</summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Largest frame accepted, in bytes. Anything larger closes the connection. Default 4 MB.</summary>
    public int MaxMessageSize { get; set; } = 4 * 1024 * 1024;

    /// <summary>Initial size of the pooled receive buffer. It grows to fit and stays grown. Default 4 KB.</summary>
    public int ReceiveBufferSize { get; set; } = 4 * 1024;

    /// <summary>How many handlers may run at once on one connection. Default 64.</summary>
    /// <remarks>
    /// Invocations are dispatched off the receive pump so a slow handler cannot stall the socket.
    /// This is the backpressure valve: once the limit is reached the pump stops reading.
    /// </remarks>
    public int MaxConcurrentInvocations { get; set; } = 64;

    /// <summary>Serialisation used for arguments and return values.</summary>
    public JsonSerializerOptions JsonOptions { get; set; } = Default;

    /// <summary>camelCase, nulls omitted - what a browser client expects to receive.</summary>
    public static JsonSerializerOptions Default { get; } = new(JsonSerializerDefaults.Web)
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxMessageSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ReceiveBufferSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxConcurrentInvocations);
    }
}

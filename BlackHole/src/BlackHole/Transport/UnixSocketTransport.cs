// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Net;
using System.Net.Sockets;

namespace BlackHole.Transport;

/// <summary>
/// Connects over a Unix domain socket - two processes on one machine, addressed by a filesystem
/// path instead of a port.
/// </summary>
/// <remarks>
/// <para>
/// The wire format is identical to TCP; only the address family changes. What you gain is a shorter
/// kernel path (no IP or TCP layer, no checksums, no loopback routing) and a socket that is not
/// reachable from the network at all - the file's permissions are the access control.
/// </para>
/// <para>
/// Supported on Linux, macOS, and Windows 10 build 17063 or later.
/// </para>
/// </remarks>
public static class UnixSocketTransport
{
    /// <summary>Connects to the socket file at <paramref name="path"/>.</summary>
    /// <param name="path">Filesystem path of the socket the server is listening on.</param>
    /// <param name="options">Transport settings; defaults are used when omitted.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="startReceiving">
    /// Leave true for a transport you will use directly. Pass false to install
    /// <see cref="ITransport.Dispatcher"/> before the first message can arrive.
    /// </param>
    public static async Task<StreamTransport> ConnectAsync(
        string path,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(path), cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        return ForConnectedSocket(socket, path, options, startReceiving);
    }

    /// <summary>Connects with exponential backoff, for a client that may start before its server.</summary>
    /// <param name="path">Filesystem path of the socket.</param>
    /// <param name="attempts">How many times to try before giving up.</param>
    /// <param name="initialDelay">Delay before the second attempt; doubles up to 5 seconds.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="cancellationToken">Cancels the connect attempts.</param>
    /// <param name="startReceiving">See <see cref="ConnectAsync"/>.</param>
    public static async Task<StreamTransport> ConnectWithRetryAsync(
        string path,
        int attempts = 5,
        TimeSpan? initialDelay = null,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(attempts);

        TimeSpan delay = initialDelay ?? TimeSpan.FromMilliseconds(100);
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await ConnectAsync(path, options, cancellationToken, startReceiving).ConfigureAwait(false);
            }
            catch (Exception) when (attempt < attempts && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 5_000));
            }
        }
    }

    /// <summary>Wraps a socket the listener already accepted.</summary>
    /// <param name="socket">A connected Unix domain socket.</param>
    /// <param name="path">The socket path, for diagnostics.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="startReceiving">See <see cref="ConnectAsync"/>.</param>
    public static StreamTransport ForConnectedSocket(
        Socket socket,
        string path,
        TransportOptions? options = null,
        bool startReceiving = true)
    {
        ArgumentNullException.ThrowIfNull(socket);

        var stream = new NetworkStream(socket, ownsSocket: false);
        return new StreamTransport(
            stream,
            options,
            remoteEndPoint: $"unix:{path}",
            kind: "uds",
            isAlive: () => socket.Connected,
            onDispose: socket.Dispose,
            startReceiving: startReceiving);
    }

    /// <summary>
    /// True when this platform can use Unix domain sockets. False only on Windows builds older
    /// than 17063.
    /// </summary>
    public static bool IsSupported
    {
        get
        {
            try
            {
                using var probe = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                return true;
            }
            catch (SocketException)
            {
                return false;
            }
            catch (PlatformNotSupportedException)
            {
                return false;
            }
        }
    }

    /// <summary>
    /// A socket path under the system temp directory, for callers that just need a private channel.
    /// </summary>
    /// <remarks>
    /// Unix caps the path at around 100 bytes, so this stays short deliberately rather than nesting
    /// under a descriptive directory.
    /// </remarks>
    public static string TempPath(string name) =>
        Path.Combine(Path.GetTempPath(), $"bh-{name}.sock");
}

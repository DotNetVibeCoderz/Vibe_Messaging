// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.IO.Pipes;

namespace BlackHole.Transport;

/// <summary>
/// Connects over a named pipe - the idiomatic same-machine channel on Windows.
/// </summary>
/// <remarks>
/// <para>
/// The wire format is identical to TCP. On Windows this is a real kernel named pipe, addressed by
/// name rather than port and secured by the pipe's ACL. On Linux and macOS, .NET implements named
/// pipes over Unix domain sockets in a temp directory, so this and
/// <see cref="UnixSocketTransport"/> end up in the same place - use whichever names the endpoint
/// the way your deployment thinks about it.
/// </para>
/// <para>
/// Pipes are opened in byte mode, not message mode: BlackHole does its own framing, and message
/// mode would impose a second, redundant one.
/// </para>
/// </remarks>
public static class NamedPipeTransport
{
    /// <summary>Connects to a named pipe on this machine.</summary>
    /// <param name="pipeName">The pipe name, without the <c>\\.\pipe\</c> prefix.</param>
    /// <param name="options">Transport settings; defaults are used when omitted.</param>
    /// <param name="timeout">How long to wait for the pipe to exist. Default 10 seconds.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="startReceiving">
    /// Leave true for a transport you will use directly. Pass false to install
    /// <see cref="ITransport.Dispatcher"/> before the first message can arrive.
    /// </param>
    public static Task<StreamTransport> ConnectAsync(
        string pipeName,
        TransportOptions? options = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true) =>
        ConnectAsync(".", pipeName, options, timeout, cancellationToken, startReceiving);

    /// <summary>Connects to a named pipe, optionally on another machine.</summary>
    /// <param name="serverName">Machine hosting the pipe; "." for this one.</param>
    /// <param name="pipeName">The pipe name, without the <c>\\.\pipe\</c> prefix.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="timeout">How long to wait for the pipe to exist. Default 10 seconds.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="startReceiving">See the other overload.</param>
    public static async Task<StreamTransport> ConnectAsync(
        string serverName,
        string pipeName,
        TransportOptions? options = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipeName);

        var pipe = new NamedPipeClientStream(
            serverName,
            pipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous | PipeOptions.WriteThrough);

        try
        {
            await pipe.ConnectAsync((int)(timeout ?? TimeSpan.FromSeconds(10)).TotalMilliseconds, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await pipe.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return new StreamTransport(
            pipe,
            options,
            remoteEndPoint: $"pipe:{pipeName}",
            kind: "pipe",
            isAlive: () => pipe.IsConnected,
            startReceiving: startReceiving);
    }

    /// <summary>Connects with exponential backoff, for a client that may start before its server.</summary>
    /// <param name="pipeName">The pipe name.</param>
    /// <param name="attempts">How many times to try before giving up.</param>
    /// <param name="initialDelay">Delay before the second attempt; doubles up to 5 seconds.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="cancellationToken">Cancels the connect attempts.</param>
    /// <param name="startReceiving">See <see cref="ConnectAsync(string, TransportOptions, TimeSpan?, CancellationToken, bool)"/>.</param>
    public static async Task<StreamTransport> ConnectWithRetryAsync(
        string pipeName,
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
                return await ConnectAsync(pipeName, options, TimeSpan.FromSeconds(2), cancellationToken, startReceiving)
                    .ConfigureAwait(false);
            }
            catch (Exception) when (attempt < attempts && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 5_000));
            }
        }
    }

    /// <summary>Wraps a server-side pipe that has already accepted a client.</summary>
    /// <param name="pipe">A connected pipe stream.</param>
    /// <param name="pipeName">The pipe name, for diagnostics.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="startReceiving">See <see cref="ConnectAsync(string, TransportOptions, TimeSpan?, CancellationToken, bool)"/>.</param>
    public static StreamTransport ForConnectedPipe(
        PipeStream pipe,
        string pipeName,
        TransportOptions? options = null,
        bool startReceiving = true)
    {
        ArgumentNullException.ThrowIfNull(pipe);

        return new StreamTransport(
            pipe,
            options,
            remoteEndPoint: $"pipe:{pipeName}",
            kind: "pipe",
            isAlive: () => pipe.IsConnected,
            startReceiving: startReceiving);
    }
}

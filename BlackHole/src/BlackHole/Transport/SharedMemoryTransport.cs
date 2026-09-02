// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.Transport;

/// <summary>
/// Connects two processes on one machine through shared memory - no socket, no syscall per message.
/// </summary>
/// <remarks>
/// <para>
/// A named segment holds one lock-free ring per direction. Sending is a copy into the ring and a
/// cursor advance; receiving is a copy out and a cursor advance. The kernel is not involved once
/// the segment is mapped, which is where the latency goes.
/// </para>
/// <para>
/// The cost is that nothing parks a waiting thread for you. An idle endpoint polls, so a
/// shared-memory link burns some CPU where a socket would sleep. Tune that with
/// <see cref="SharedMemoryOptions.SpinCount"/> and <see cref="SharedMemoryOptions.PollInterval"/>,
/// and prefer a socket when you have many mostly-idle connections.
/// </para>
/// <para>
/// One segment carries exactly one connection. For several clients, give each its own segment name -
/// which is what <see cref="SharedMemoryListenerHost"/> does.
/// </para>
/// </remarks>
public static class SharedMemoryTransport
{
    /// <summary>
    /// Creates a segment and waits for a peer to open it. This is the server side.
    /// </summary>
    /// <param name="name">Segment name both sides agree on.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="shared">Ring capacity and waiting strategy.</param>
    /// <param name="startReceiving">
    /// Leave true for a transport you will use directly. Pass false to install
    /// <see cref="ITransport.Dispatcher"/> before the first message can arrive.
    /// </param>
    public static StreamTransport Create(
        string name,
        TransportOptions? options = null,
        SharedMemoryOptions? shared = null,
        bool startReceiving = true)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        shared ??= new SharedMemoryOptions();

        SharedMemorySegment segment = SharedMemorySegment.Create(name, shared.RingCapacity);
        return Wrap(segment, name, options, shared, startReceiving);
    }

    /// <summary>Opens a segment the peer already created. This is the client side.</summary>
    /// <param name="name">Segment name both sides agree on.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="shared">Waiting strategy. The capacity is read from the segment.</param>
    /// <param name="startReceiving">See <see cref="Create"/>.</param>
    public static StreamTransport Open(
        string name,
        TransportOptions? options = null,
        SharedMemoryOptions? shared = null,
        bool startReceiving = true)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        shared ??= new SharedMemoryOptions();

        SharedMemorySegment segment = SharedMemorySegment.Open(name);
        return Wrap(segment, name, options, shared, startReceiving);
    }

    /// <summary>Opens a segment, waiting for the peer to create it first.</summary>
    /// <param name="name">Segment name.</param>
    /// <param name="timeout">How long to wait for the segment to appear. Default 10 seconds.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="shared">Waiting strategy.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <param name="startReceiving">See <see cref="Create"/>.</param>
    public static async Task<StreamTransport> OpenWithRetryAsync(
        string name,
        TimeSpan? timeout = null,
        TransportOptions? options = null,
        SharedMemoryOptions? shared = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        TimeSpan deadline = timeout ?? TimeSpan.FromSeconds(10);
        DateTime expires = DateTime.UtcNow + deadline;
        Exception? last = null;

        while (DateTime.UtcNow < expires)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return Open(name, options, shared, startReceiving);
            }
            catch (Exception ex) when (ex is FileNotFoundException or IOException or UnauthorizedAccessException)
            {
                last = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(20), cancellationToken).ConfigureAwait(false);
            }
        }

        throw new TimeoutException($"Shared memory segment '{name}' did not appear within {deadline}.", last);
    }

    /// <summary>
    /// Connects to a <see cref="SharedMemoryListenerHost"/> by claiming a free slot from its pool.
    /// </summary>
    /// <remarks>
    /// Slots are scanned in order and claimed atomically, so several clients racing for the same
    /// pool each end up on a different segment. A busy slot is not an error - it just means try the
    /// next one.
    /// </remarks>
    /// <param name="name">The listener's base segment name.</param>
    /// <param name="slots">Pool size the listener was created with. Default 8.</param>
    /// <param name="timeout">How long to keep retrying the whole pool. Default 10 seconds.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="shared">Waiting strategy.</param>
    /// <param name="cancellationToken">Cancels the attempt.</param>
    /// <param name="startReceiving">See <see cref="Create"/>.</param>
    /// <exception cref="TimeoutException">Every slot stayed busy or absent until the deadline.</exception>
    public static async Task<StreamTransport> ConnectAsync(
        string name,
        int slots = 8,
        TimeSpan? timeout = null,
        TransportOptions? options = null,
        SharedMemoryOptions? shared = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(slots);

        TimeSpan deadline = timeout ?? TimeSpan.FromSeconds(10);
        DateTime expires = DateTime.UtcNow + deadline;
        Exception? last = null;

        while (DateTime.UtcNow < expires)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (int slot = 0; slot < slots; slot++)
            {
                try
                {
                    return Open(SharedMemoryListenerHost.SlotName(name, slot), options, shared, startReceiving);
                }
                catch (SegmentBusyException ex)
                {
                    last = ex;   // Taken by another client; the next slot may be free.
                }
                catch (Exception ex) when (ex is FileNotFoundException or IOException or UnauthorizedAccessException)
                {
                    last = ex;   // The listener has not created this slot yet.
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"No free shared memory slot under '{name}' within {deadline}. All {slots} were busy or absent.", last);
    }

    private static StreamTransport Wrap(
        SharedMemorySegment segment,
        string name,
        TransportOptions? options,
        SharedMemoryOptions shared,
        bool startReceiving)
    {
        var stream = new SharedMemoryStream(segment, shared);

        // Keepalive over shared memory is pointless overhead: liveness is a flag in the header that
        // costs a volatile read, and the peer cannot "go quiet" without the OS unmapping it.
        TransportOptions transportOptions = (options ?? new TransportOptions()).Clone();
        transportOptions.KeepAliveInterval = null;

        return new StreamTransport(
            stream,
            transportOptions,
            remoteEndPoint: $"shm:{name}",
            kind: "shm",
            isAlive: () => stream.IsPeerAlive,
            // The ring has nothing to park on, so its read loop spins. That must happen on a
            // thread of its own rather than one borrowed from the pool.
            dedicatedReceiveThread: true,
            startReceiving: startReceiving);
    }

    /// <summary>
    /// Removes the backing file for a segment on platforms that use one. A no-op on Windows, where
    /// the segment lives in the kernel's namespace and disappears with its last handle.
    /// </summary>
    /// <remarks>
    /// Worth calling after an unclean shutdown on Linux or macOS: a leftover file makes the next
    /// <see cref="Open"/> succeed against a segment nobody is serving.
    /// </remarks>
    public static void Cleanup(string name)
    {
        if (OperatingSystem.IsWindows())
            return;

        try
        {
            string path = SharedMemorySegment.BackingPath(name);
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception)
        {
            // Best effort: another process may still hold it, which is not our problem to solve.
        }
    }
}

// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;

namespace BlackHole.Transport;

/// <summary>
/// Accepts shared-memory connections from a fixed pool of segments.
/// </summary>
/// <remarks>
/// <para>
/// A segment carries exactly one connection, so a listener is a pool: it creates
/// <c>{name}-0</c> through <c>{name}-{slots-1}</c> up front, and a client claims a free one with an
/// atomic compare-and-exchange on its liveness flag. A slot is recycled - torn down and recreated
/// with fresh cursors - once its connection ends.
/// </para>
/// <para>
/// The pool is bounded on purpose, and small by default. Every slot costs
/// <c>2 x RingCapacity</c> of resident memory whether it is in use or not, and an idle
/// shared-memory endpoint still polls. If you need hundreds of mostly-idle connections, that is
/// what sockets are for; shared memory is for a handful of links that are worth the memory and the
/// CPU.
/// </para>
/// </remarks>
public sealed class SharedMemoryListenerHost : IListenerHost
{
    private sealed class Slot
    {
        public required int Index { get; init; }
        public required string Name { get; init; }
        public StreamTransport? Transport { get; set; }
        public bool Occupied { get; set; }
    }

    private readonly string _name;
    private readonly TransportOptions _options;
    private readonly SharedMemoryOptions _shared;
    private readonly Slot[] _slots;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, ITransport> _connections = new();
    private readonly Lock _slotLock = new();
    private Task? _acceptLoop;

    /// <param name="name">Base segment name; slots are <c>{name}-0</c>, <c>{name}-1</c>, and so on.</param>
    /// <param name="slots">How many simultaneous connections to make room for. Default 8.</param>
    /// <param name="options">Transport settings applied to every accepted connection.</param>
    /// <param name="shared">Ring capacity and waiting strategy.</param>
    public SharedMemoryListenerHost(
        string name,
        int slots = 8,
        TransportOptions? options = null,
        SharedMemoryOptions? shared = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(slots);

        _name = name;
        _options = options ?? new TransportOptions();
        _shared = shared ?? new SharedMemoryOptions();
        _slots = Enumerable.Range(0, slots)
            .Select(i => new Slot { Index = i, Name = SlotName(name, i) })
            .ToArray();

        MaxConnections = slots;
    }

    /// <inheritdoc />
    public string Endpoint => $"shm:{_name}[{_slots.Length}]";

    /// <summary>Base segment name.</summary>
    public string Name => _name;

    /// <summary>Segments in the pool.</summary>
    public int SlotCount => _slots.Length;

    /// <inheritdoc />
    public int ConnectionCount => _connections.Count;

    /// <inheritdoc />
    public int MaxConnections { get; set; }

    /// <summary>How often the accept loop checks the pool for newly claimed slots. Default 1 ms.</summary>
    public TimeSpan AcceptPollInterval { get; set; } = TimeSpan.FromMilliseconds(1);

    /// <inheritdoc />
    public event Action<ITransport>? TransportConnected;

    /// <inheritdoc />
    public event Action<ITransport, Exception?>? TransportDisconnected;

    /// <summary>The segment name for one slot.</summary>
    public static string SlotName(string name, int index) => $"{name}-{index}";

    /// <inheritdoc />
    /// <param name="backlog">Ignored: the pool size is the backlog.</param>
    public void Start(int backlog = 512)
    {
        lock (_slotLock)
        {
            foreach (Slot slot in _slots)
                CreateSegment(slot);
        }

        _acceptLoop = Task.Run(() => AcceptLoopAsync(_shutdown.Token));
    }

    /// <summary>
    /// Creates (or recreates) a slot's segment, unclaimed and with zeroed cursors.
    /// </summary>
    private void CreateSegment(Slot slot)
    {
        try
        {
            // A leftover file from an unclean shutdown would otherwise be reopened with stale
            // cursors and a stale claim.
            SharedMemoryTransport.Cleanup(slot.Name);

            slot.Transport = SharedMemoryTransport.Create(
                slot.Name, _options, _shared, startReceiving: false);
            slot.Occupied = false;
        }
        catch (Exception ex)
        {
            _options.ErrorHandler?.Invoke(ex);
            slot.Transport = null;
        }
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            bool sawWork = false;

            foreach (Slot slot in _slots)
            {
                StreamTransport? transport;
                lock (_slotLock)
                {
                    // A slot with no peer yet, or one already handed out, is not work.
                    if (slot.Occupied || slot.Transport is null || !slot.Transport.IsConnected)
                        continue;

                    if (_connections.Count >= MaxConnections)
                        continue;

                    slot.Occupied = true;
                    transport = slot.Transport;
                }

                sawWork = true;
                _connections[transport.Id] = transport;
                transport.Closed += (t, failure) => OnTransportClosed(slot, t, failure);

                try
                {
                    // Handed over unstarted, so the handler can install a dispatcher before the
                    // first frame is delivered.
                    TransportConnected?.Invoke(transport);
                    transport.Start();
                }
                catch (Exception ex)
                {
                    _options.ErrorHandler?.Invoke(ex);
                    await transport.DisposeAsync().ConfigureAwait(false);
                }
            }

            if (!sawWork)
                await Task.Delay(AcceptPollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    private void OnTransportClosed(Slot slot, ITransport transport, Exception? failure)
    {
        if (!_connections.TryRemove(transport.Id, out ITransport? removed))
            return;

        TransportDisconnected?.Invoke(removed, failure);

        // Recycle the slot on a background task: this runs on the transport's own close path, and
        // tearing that transport down from inside its own event would deadlock.
        _ = Task.Run(async () =>
        {
            try
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception) { }

            if (_shutdown.IsCancellationRequested)
                return;

            lock (_slotLock)
            {
                if (!_shutdown.IsCancellationRequested)
                    CreateSegment(slot);
            }
        });
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try { await _shutdown.CancelAsync().ConfigureAwait(false); } catch (ObjectDisposedException) { }

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        StreamTransport?[] transports;
        lock (_slotLock)
        {
            transports = _slots.Select(s => s.Transport).ToArray();
            foreach (Slot slot in _slots)
                slot.Transport = null;
        }

        foreach (StreamTransport? transport in transports)
        {
            if (transport is null) continue;
            try { await transport.DisposeAsync().ConfigureAwait(false); } catch (Exception) { }
        }

        _connections.Clear();

        foreach (Slot slot in _slots)
            SharedMemoryTransport.Cleanup(slot.Name);

        _shutdown.Dispose();
    }
}

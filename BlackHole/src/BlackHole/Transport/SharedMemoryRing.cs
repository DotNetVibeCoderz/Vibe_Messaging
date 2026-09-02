// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.IO.MemoryMappedFiles;
using BlackHole.Protocol;

namespace BlackHole.Transport;

/// <summary>
/// A single-producer, single-consumer byte ring living in shared memory.
/// </summary>
/// <remarks>
/// <para>
/// One writer and one reader, each in a different process, coordinating through two monotonically
/// increasing cursors. The writer only ever advances the write cursor; the reader only ever advances
/// the read cursor. Neither ever moves the other's, so no lock is needed and no compare-and-swap
/// either - a plain volatile read of the peer's cursor is enough to know how much space or data
/// there is.
/// </para>
/// <para>
/// The two cursors sit on separate 64-byte cache lines. Sharing a line would put the reader and
/// writer in a permanent cache-coherence fight over the same line, which costs far more than the
/// 64 bytes of padding saves.
/// </para>
/// <para>
/// Cursors are <see cref="long"/> and never wrap in practice: at 10 GB/s it would take about 29
/// years. The data index is <c>cursor &amp; (capacity - 1)</c>, so the capacity is always a power
/// of two.
/// </para>
/// </remarks>
internal sealed unsafe class SharedMemoryRing
{
    /// <summary>Bytes reserved for one cursor, sized to a cache line to avoid false sharing.</summary>
    internal const int CursorSlotSize = 64;

    private readonly byte* _base;
    private readonly byte* _data;
    private readonly long* _writeCursor;
    private readonly long* _readCursor;
    private readonly int _capacity;
    private readonly int _mask;

    /// <param name="ringBase">Pointer to this ring's control block.</param>
    /// <param name="capacity">Data bytes, a power of two.</param>
    internal SharedMemoryRing(byte* ringBase, int capacity)
    {
        _base = ringBase;
        _capacity = capacity;
        _mask = capacity - 1;

        _writeCursor = (long*)ringBase;
        _readCursor = (long*)(ringBase + CursorSlotSize);
        _data = ringBase + (CursorSlotSize * 2);
    }

    /// <summary>Total bytes this ring can hold.</summary>
    internal int Capacity => _capacity;

    /// <summary>Control block plus data, for laying out the mapped file.</summary>
    internal static int TotalSize(int capacity) => (CursorSlotSize * 2) + capacity;

    /// <summary>Bytes written but not yet read.</summary>
    internal long Available => Volatile.Read(ref *_writeCursor) - Volatile.Read(ref *_readCursor);

    /// <summary>Bytes that can be written before the ring is full.</summary>
    internal long Free => _capacity - Available;

    /// <summary>Zeroes both cursors. Only safe before either side starts.</summary>
    internal void Reset()
    {
        Volatile.Write(ref *_writeCursor, 0);
        Volatile.Write(ref *_readCursor, 0);
    }

    /// <summary>
    /// Copies as much of <paramref name="source"/> into the ring as fits, and returns how much that
    /// was. Zero means the ring is full and the caller should wait.
    /// </summary>
    internal int TryWrite(ReadOnlySpan<byte> source)
    {
        if (source.IsEmpty)
            return 0;

        long write = Volatile.Read(ref *_writeCursor);
        long free = _capacity - (write - Volatile.Read(ref *_readCursor));
        if (free <= 0)
            return 0;

        int count = (int)Math.Min(source.Length, free);
        int offset = (int)(write & _mask);
        int firstPart = Math.Min(count, _capacity - offset);

        // Two copies when the write wraps past the end of the buffer.
        source[..firstPart].CopyTo(new Span<byte>(_data + offset, firstPart));
        if (count > firstPart)
            source.Slice(firstPart, count - firstPart).CopyTo(new Span<byte>(_data, count - firstPart));

        // The data must be visible before the cursor that publishes it.
        Volatile.Write(ref *_writeCursor, write + count);
        return count;
    }

    /// <summary>
    /// Copies as much as is available into <paramref name="destination"/>, and returns how much that
    /// was. Zero means the ring is empty and the caller should wait.
    /// </summary>
    internal int TryRead(Span<byte> destination)
    {
        if (destination.IsEmpty)
            return 0;

        long read = Volatile.Read(ref *_readCursor);
        long available = Volatile.Read(ref *_writeCursor) - read;
        if (available <= 0)
            return 0;

        int count = (int)Math.Min(destination.Length, available);
        int offset = (int)(read & _mask);
        int firstPart = Math.Min(count, _capacity - offset);

        new ReadOnlySpan<byte>(_data + offset, firstPart).CopyTo(destination);
        if (count > firstPart)
            new ReadOnlySpan<byte>(_data, count - firstPart).CopyTo(destination[firstPart..]);

        // Only release the space once the bytes have been copied out.
        Volatile.Write(ref *_readCursor, read + count);
        return count;
    }
}

/// <summary>
/// The shared-memory segment two processes map: a header plus one ring per direction.
/// </summary>
/// <remarks>
/// <code>
/// +--------------------+   header: magic, version, capacity, liveness flags
/// | Header (64 bytes)  |
/// +--------------------+
/// | Ring A to B        |   write cursor (64) | read cursor (64) | data (capacity)
/// +--------------------+
/// | Ring B to A        |   same again
/// +--------------------+
/// </code>
/// The server owns ring A and reads ring B; the client does the reverse. Which side you are decides
/// which ring you write to, and that is the only asymmetry.
/// </remarks>
internal sealed unsafe class SharedMemorySegment : IDisposable
{
    /// <summary>"BHSM" - identifies the segment and catches a stale or foreign mapping.</summary>
    internal const int Magic = 0x4D534842;

    /// <summary>Layout version. A mismatch is a hard failure, never a best-effort parse.</summary>
    internal const int Version = 1;

    internal const int HeaderSize = 64;

    // Header field offsets.
    private const int MagicOffset = 0;
    private const int VersionOffset = 4;
    private const int CapacityOffset = 8;
    private const int ServerAliveOffset = 12;
    private const int ClientAliveOffset = 16;

    private readonly MemoryMappedFile _file;
    private readonly MemoryMappedViewAccessor _view;
    private readonly byte* _base;
    private readonly bool _isServer;
    private bool _disposed;
    private bool _released;
    private bool _claimed = true;

    private SharedMemorySegment(MemoryMappedFile file, MemoryMappedViewAccessor view, int capacity, bool isServer)
    {
        _file = file;
        _view = view;
        _isServer = isServer;

        byte* pointer = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
        _base = pointer;

        byte* ringA = _base + HeaderSize;
        byte* ringB = ringA + SharedMemoryRing.TotalSize(capacity);

        // The server writes A and reads B; the client is the mirror image.
        Outbound = new SharedMemoryRing(isServer ? ringA : ringB, capacity);
        Inbound = new SharedMemoryRing(isServer ? ringB : ringA, capacity);
    }

    /// <summary>The ring this side writes into.</summary>
    internal SharedMemoryRing Outbound { get; }

    /// <summary>The ring this side reads from.</summary>
    internal SharedMemoryRing Inbound { get; }

    /// <summary>False once the peer has marked itself gone.</summary>
    internal bool IsPeerAlive =>
        Volatile.Read(ref *(int*)(_base + (_isServer ? ClientAliveOffset : ServerAliveOffset))) != 0;

    /// <summary>Total mapped size for a given ring capacity.</summary>
    internal static long SizeFor(int capacity) =>
        HeaderSize + (2L * SharedMemoryRing.TotalSize(capacity));

    /// <summary>Creates the segment and initialises its header. The server side does this.</summary>
    internal static SharedMemorySegment Create(string name, int capacity)
    {
        capacity = RoundUpToPowerOfTwo(capacity);
        long size = SizeFor(capacity);

        MemoryMappedFile file = CreateOrOpenBacking(name, size, create: true);
        MemoryMappedViewAccessor view = file.CreateViewAccessor(0, size, MemoryMappedFileAccess.ReadWrite);

        var segment = new SharedMemorySegment(file, view, capacity, isServer: true);
        segment.Outbound.Reset();
        segment.Inbound.Reset();

        Volatile.Write(ref *(int*)(segment._base + CapacityOffset), capacity);
        Volatile.Write(ref *(int*)(segment._base + VersionOffset), Version);
        Volatile.Write(ref *(int*)(segment._base + ClientAliveOffset), 0);
        Volatile.Write(ref *(int*)(segment._base + ServerAliveOffset), 1);
        // Magic goes last: a client polling for it will not see a half-built header.
        Volatile.Write(ref *(int*)(segment._base + MagicOffset), Magic);

        return segment;
    }

    /// <summary>
    /// Opens a segment the server already created and claims it for this client.
    /// </summary>
    /// <remarks>
    /// The claim is a compare-and-exchange on the client-alive flag, which lives in the shared
    /// mapping and is therefore atomic across processes. Two clients racing for the same segment
    /// cannot both win: exactly one sees the flag go from 0 to 1, and the loser moves to the next
    /// slot. This is what makes a pool of segments safe to scan.
    /// </remarks>
    /// <exception cref="SegmentBusyException">Another client already holds this segment.</exception>
    internal static SharedMemorySegment Open(string name)
    {
        MemoryMappedFile file = CreateOrOpenBacking(name, size: 0, create: false);

        // Read the header first to learn the capacity, then map the whole thing.
        int capacity;
        using (MemoryMappedViewAccessor probe = file.CreateViewAccessor(0, HeaderSize, MemoryMappedFileAccess.Read))
        {
            int magic = probe.ReadInt32(MagicOffset);
            if (magic != Magic)
                throw new BlackHoleProtocolException(
                    $"Shared memory segment '{name}' is not a BlackHole segment (magic 0x{magic:X8}).");

            int version = probe.ReadInt32(VersionOffset);
            if (version != Version)
                throw new BlackHoleProtocolException(
                    $"Shared memory segment '{name}' is layout version {version}; this build speaks {Version}.");

            capacity = probe.ReadInt32(CapacityOffset);
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
                throw new BlackHoleProtocolException(
                    $"Shared memory segment '{name}' declares an invalid capacity of {capacity}.");
        }

        long size = SizeFor(capacity);
        MemoryMappedViewAccessor view = file.CreateViewAccessor(0, size, MemoryMappedFileAccess.ReadWrite);

        var segment = new SharedMemorySegment(file, view, capacity, isServer: false);

        if (Interlocked.CompareExchange(ref *(int*)(segment._base + ClientAliveOffset), 1, 0) != 0)
        {
            // Lost the race, or the slot was already in use. Release without touching the flag -
            // it belongs to whoever won.
            segment._claimed = false;
            segment.Dispose();
            throw new SegmentBusyException(name);
        }

        segment._claimed = true;
        return segment;
    }

    private static MemoryMappedFile CreateOrOpenBacking(string name, long size, bool create)
    {
        if (OperatingSystem.IsWindows())
        {
            // Windows has a real named shared-memory namespace, backed by the page file.
            return create
                ? MemoryMappedFile.CreateOrOpen(name, size, MemoryMappedFileAccess.ReadWrite)
                : MemoryMappedFile.OpenExisting(name, MemoryMappedFileRights.ReadWrite);
        }

        // Elsewhere, back it with a file the two processes agree on. /dev/shm is a tmpfs on Linux,
        // so this stays in memory; on macOS it falls back to the temp directory.
        string path = BackingPath(name);
        if (create)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            using (var stream = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
                stream.SetLength(size);
        }
        else if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Shared memory segment '{name}' does not exist.", path);
        }

        return MemoryMappedFile.CreateFromFile(path, FileMode.Open, mapName: null, capacity: 0,
            MemoryMappedFileAccess.ReadWrite);
    }

    /// <summary>Where a named segment lives on platforms without a shared-memory namespace.</summary>
    internal static string BackingPath(string name)
    {
        string directory = Directory.Exists("/dev/shm") ? "/dev/shm" : Path.GetTempPath();
        return Path.Combine(directory, $"blackhole-{name}");
    }

    private static int RoundUpToPowerOfTwo(int value)
    {
        int size = 4096;
        int target = Math.Clamp(value, 4096, 1 << 30);
        while (size < target) size <<= 1;
        return size;
    }

    /// <summary>Marks this side gone, so the peer's next poll sees the disconnect.</summary>
    /// <summary>
    /// Clears this side's liveness flag, so the peer's next poll sees the disconnect.
    /// </summary>
    /// <remarks>
    /// This is the only end-of-stream signal shared memory has. A socket read returns 0 when the
    /// peer closes; a ring just goes quiet, which is indistinguishable from an idle peer. Clearing
    /// the flag is what lets the other side tell those apart, so it must happen before the mapping
    /// is torn down - and must not be skipped just because dispose is already under way.
    /// </remarks>
    internal void MarkClosed()
    {
        // A client that lost the claim race never owned the flag, so it must not clear one that
        // belongs to whoever won.
        if (_released || (!_isServer && !_claimed))
            return;

        try
        {
            Volatile.Write(ref *(int*)(_base + (_isServer ? ServerAliveOffset : ClientAliveOffset)), 0);
        }
        catch (Exception)
        {
            // The view may already be gone; the peer will notice through its own liveness flag.
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        // Publish the disconnect while the mapping is still valid, then release it. _released
        // guards MarkClosed rather than _disposed, so this call is not a no-op against itself.
        MarkClosed();
        _released = true;

        try { _view.SafeMemoryMappedViewHandle.ReleasePointer(); } catch (Exception) { }
        _view.Dispose();
        _file.Dispose();
    }
}

/// <summary>
/// Thrown when a shared-memory segment is already claimed by another client. Not an error at the
/// pool level - it simply means "try the next slot".
/// </summary>
public sealed class SegmentBusyException : Exception
{
    public SegmentBusyException(string name)
        : base($"Shared memory segment '{name}' is already in use by another client.") => SegmentName = name;

    /// <summary>The segment that was busy.</summary>
    public string SegmentName { get; }
}

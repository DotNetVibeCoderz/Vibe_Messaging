// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;

namespace SocketSignal.Buffers;

/// <summary>
/// A growable <see cref="IBufferWriter{T}"/> backed by <see cref="ArrayPool{T}.Shared"/>.
/// </summary>
/// <remarks>
/// One of these lives for the lifetime of a connection and is reused for every outbound frame:
/// <see cref="Reset"/> keeps the rented array and only rewinds the cursor, which is what makes a
/// long-lived sender allocation free after warm-up. It is deliberately not thread safe - the
/// connection holds its send lock while writing.
/// </remarks>
public sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;
    private bool _disposed;

    public PooledBufferWriter(int initialCapacity = 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(initialCapacity);
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    /// <summary>Bytes written since the last <see cref="Reset"/>.</summary>
    public int WrittenCount => _written;

    /// <summary>Current rented capacity. Grows by doubling, never shrinks before <see cref="Dispose"/>.</summary>
    public int Capacity => _buffer.Length;

    /// <summary>What has been written so far. Invalid after the next write, <see cref="Reset"/>, or dispose.</summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);

    /// <inheritdoc cref="WrittenMemory"/>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (_written + count > _buffer.Length)
            throw new InvalidOperationException("Advanced past the end of the buffer.");
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    /// <summary>Rewinds to empty, keeping the rented array for the next frame.</summary>
    public void Reset() => _written = 0;

    private void EnsureCapacity(int sizeHint)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (sizeHint <= 0) sizeHint = 1;
        int required = _written + sizeHint;
        if (required <= _buffer.Length)
            return;

        int newSize = Math.Max(required, _buffer.Length * 2);
        byte[] next = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _written).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = [];
    }
}

// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;

namespace BlackHole.Buffers;

/// <summary>
/// A growable <see cref="IBufferWriter{T}"/> backed by <see cref="ArrayPool{T}.Shared"/>.
/// </summary>
/// <remarks>
/// Used wherever the library needs a scratch buffer whose final size is unknown - batch envelopes,
/// stream reassembly, benchmark harnesses. It is a struct-free class so it can be reused across
/// calls: <see cref="Reset"/> keeps the rented array and only moves the write cursor back to zero,
/// which is what makes a long-lived sender allocation free after warm-up.
/// </remarks>
public sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;
    private bool _disposed;

    public PooledBufferWriter(int initialCapacity = 4096)
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

    /// <summary>Rewinds to empty, keeping the rented array for the next message.</summary>
    public void Reset() => _written = 0;

    /// <summary>Copies the written bytes into a fresh array. Only for callers that need ownership.</summary>
    public byte[] ToArray() => WrittenSpan.ToArray();

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

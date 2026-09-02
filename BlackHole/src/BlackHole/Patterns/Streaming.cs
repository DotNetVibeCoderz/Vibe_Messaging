// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using BlackHole.Buffers;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Patterns;

/// <summary>
/// Sends a large body as StreamStart, N x StreamChunk, StreamEnd.
/// </summary>
/// <remarks>
/// The chunk buffer is rented once for the whole transfer, and chunks are written without flushing
/// until <see cref="FlushThreshold"/> bytes are pending. That turns "one socket write per 4 KiB"
/// into one write per 64 KiB while still bounding how much sits unflushed - the point v2 missed by
/// flushing every chunk.
/// </remarks>
public sealed class StreamSender
{
    private readonly ITransport _transport;

    public StreamSender(ITransport transport) =>
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));

    /// <summary>Bytes to accumulate before pushing to the socket. Default 64 KiB.</summary>
    public int FlushThreshold { get; set; } = 64 * 1024;

    /// <summary>Streams <paramref name="source"/> to the peer and returns the bytes sent.</summary>
    public async Task<long> SendAsync(
        string streamId,
        Stream source,
        StreamDescriptor? descriptor = null,
        int chunkSize = 4096,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, 64);

        StreamDescriptor meta = descriptor ?? new StreamDescriptor(
            streamId,
            source.CanSeek ? source.Length - source.Position : StreamDescriptor.UnknownLength,
            "application/octet-stream");

        await _transport.SendAsync(
            new BlackHoleMessage(MessageType.StreamStart, streamId, meta.Encode()),
            cancellationToken).ConfigureAwait(false);

        byte[] buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
        long totalSent = 0;
        long chunkIndex = 0;
        int pendingBytes = 0;

        try
        {
            while (true)
            {
                int read = await source.ReadAsync(buffer.AsMemory(0, chunkSize), cancellationToken).ConfigureAwait(false);
                if (read <= 0)
                    break;

                await _transport.WriteAsync(
                    new BlackHoleMessage(MessageType.StreamChunk, streamId, buffer.AsMemory(0, read), chunkIndex++),
                    cancellationToken).ConfigureAwait(false);

                totalSent += read;
                pendingBytes += read;
                if (pendingBytes >= FlushThreshold)
                {
                    await _transport.FlushAsync(cancellationToken).ConfigureAwait(false);
                    pendingBytes = 0;
                    progress?.Report(totalSent);
                }
            }

            await _transport.SendAsync(
                new BlackHoleMessage(MessageType.StreamEnd, streamId, default, chunkIndex),
                cancellationToken).ConfigureAwait(false);
            progress?.Report(totalSent);
            return totalSent;
        }
        catch (Exception ex)
        {
            await AbortAsync(streamId, ex.Message, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Convenience wrapper for a body already in memory.</summary>
    public Task<long> SendAsync(
        string streamId,
        ReadOnlyMemory<byte> data,
        StreamDescriptor? descriptor = null,
        int chunkSize = 4096,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var source = new ReadOnlyMemoryStream(data);
        return SendAsync(
            streamId, source,
            descriptor ?? new StreamDescriptor(streamId, data.Length, "application/octet-stream"),
            chunkSize, progress, cancellationToken);
    }

    /// <summary>Tells the peer to discard a stream in progress.</summary>
    public ValueTask AbortAsync(string streamId, string reason, CancellationToken cancellationToken = default) =>
        _transport.SendAsync(
            new BlackHoleMessage(MessageType.StreamAbort, streamId, Encoding.UTF8.GetBytes(reason), 0, MessageFlags.Error),
            cancellationToken);

    private sealed class ReadOnlyMemoryStream(ReadOnlyMemory<byte> memory) : Stream
    {
        private int _position;

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => memory.Length;
        public override long Position { get => _position; set => _position = (int)value; }

        public override int Read(Span<byte> destination)
        {
            int count = Math.Min(destination.Length, memory.Length - _position);
            if (count <= 0) return 0;
            memory.Span.Slice(_position, count).CopyTo(destination);
            _position += count;
            return count;
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Read(buffer.Span));

        public override long Seek(long offset, SeekOrigin origin)
        {
            _position = (int)(origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                _ => memory.Length + offset,
            });
            return _position;
        }

        public override void Flush() { }
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}

/// <summary>What a completed stream delivered.</summary>
public sealed class StreamCompletedEventArgs : EventArgs
{
    internal StreamCompletedEventArgs(string streamId, StreamDescriptor descriptor, long length, ReadOnlyMemory<byte> data, bool buffered)
    {
        StreamId = streamId;
        Descriptor = descriptor;
        Length = length;
        Data = data;
        IsBuffered = buffered;
    }

    public string StreamId { get; }
    public StreamDescriptor Descriptor { get; }

    /// <summary>Total bytes received.</summary>
    public long Length { get; }

    /// <summary>
    /// The reassembled body when the stream was buffered in memory - valid only inside the event
    /// handler, because the buffer returns to the pool as soon as it returns. Empty when the stream
    /// was written to a sink.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; }

    /// <summary>False when the payload went to a sink stream instead of memory.</summary>
    public bool IsBuffered { get; }
}

/// <summary>
/// Reassembles inbound streams.
/// </summary>
/// <remarks>
/// Each stream buffers into a pooled writer, or into a <see cref="Stream"/> from
/// <see cref="SinkFactory"/> when the caller would rather not hold it in memory.
/// <see cref="MaxStreamLength"/> and <see cref="MaxConcurrentStreams"/> stop a peer - hostile or
/// merely buggy - from turning an open stream into unbounded process memory.
/// </remarks>
public sealed class StreamReceiver : IDisposable
{
    private sealed class Reassembly : IDisposable
    {
        public required StreamDescriptor Descriptor { get; init; }
        public PooledBufferWriter? Buffer { get; init; }
        public Stream? Sink { get; init; }
        public long Received { get; set; }
        public long NextChunkIndex { get; set; }

        public void Dispose()
        {
            Buffer?.Dispose();
            Sink?.Dispose();
        }
    }

    private readonly ConcurrentDictionary<string, Reassembly> _active = new(StringComparer.Ordinal);

    /// <summary>Reject a stream once it passes this many bytes. Default 256 MiB.</summary>
    public long MaxStreamLength { get; set; } = 256L * 1024 * 1024;

    /// <summary>Reject a new stream while this many are already open. Default 64.</summary>
    public int MaxConcurrentStreams { get; set; } = 64;

    /// <summary>
    /// Optional sink for inbound bytes - return a writable stream to keep the body out of memory,
    /// or null to buffer it.
    /// </summary>
    public Func<string, StreamDescriptor, Stream?>? SinkFactory { get; set; }

    /// <summary>Streams currently open.</summary>
    public int ActiveStreams => _active.Count;

    /// <summary>Raised when a StreamStart arrives.</summary>
    public event Action<string, StreamDescriptor>? Started;

    /// <summary>Raised per chunk with (streamId, bytesSoFar, totalExpectedOrMinusOne).</summary>
    public event Action<string, long, long>? Progress;

    /// <summary>Raised once a stream ends cleanly.</summary>
    public event EventHandler<StreamCompletedEventArgs>? Completed;

    /// <summary>Raised when a stream is abandoned, with the reason.</summary>
    public event Action<string, string>? Aborted;

    /// <summary>Wires this receiver into a router.</summary>
    public StreamReceiver AttachTo(MessageRouter router)
    {
        ArgumentNullException.ThrowIfNull(router);
        router.On(
            [MessageType.StreamStart, MessageType.StreamChunk, MessageType.StreamEnd, MessageType.StreamAbort],
            HandleAsync);
        return this;
    }

    /// <summary>Handles the four stream message types. Assign to a router or a dispatcher.</summary>
    public async ValueTask HandleAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        switch (message.Type)
        {
            case MessageType.StreamStart:
                OnStart(message);
                break;

            case MessageType.StreamChunk:
                await OnChunkAsync(transport, message, cancellationToken).ConfigureAwait(false);
                break;

            case MessageType.StreamEnd:
                OnEnd(message);
                break;

            case MessageType.StreamAbort:
                Abort(message.Header, message.PayloadAsString());
                break;
        }
    }

    private void OnStart(in BlackHoleMessage message)
    {
        if (_active.Count >= MaxConcurrentStreams)
        {
            Aborted?.Invoke(message.Header, $"Refused: {MaxConcurrentStreams} streams are already open.");
            return;
        }

        StreamDescriptor descriptor = StreamDescriptor.Decode(message.Payload.Span);
        Stream? sink = SinkFactory?.Invoke(message.Header, descriptor);

        var state = new Reassembly
        {
            Descriptor = descriptor,
            Sink = sink,
            Buffer = sink is null
                ? new PooledBufferWriter(descriptor.HasLength && descriptor.TotalLength < 1 << 20
                    ? (int)Math.Max(4096, descriptor.TotalLength)
                    : 64 * 1024)
                : null,
        };

        if (_active.TryAdd(message.Header, state))
            Started?.Invoke(message.Header, descriptor);
        else
            state.Dispose();
    }

    private async ValueTask OnChunkAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        if (!_active.TryGetValue(message.Header, out Reassembly? state))
            return; // Chunk for a stream we never opened, or already abandoned.

        if (message.CorrelationId != state.NextChunkIndex)
        {
            Abort(message.Header, $"Chunk {message.CorrelationId} arrived while expecting {state.NextChunkIndex}.");
            return;
        }
        state.NextChunkIndex++;

        long projected = state.Received + message.Payload.Length;
        if (projected > MaxStreamLength)
        {
            Abort(message.Header, $"Stream passed the {MaxStreamLength:N0} byte limit.");
            _ = transport; // The peer is told by the sender's own abort path; nothing to do here.
            return;
        }

        if (state.Sink is not null)
            await state.Sink.WriteAsync(message.Payload, cancellationToken).ConfigureAwait(false);
        else
            state.Buffer!.Write(message.Payload.Span);

        state.Received = projected;
        Progress?.Invoke(message.Header, state.Received, state.Descriptor.TotalLength);
    }

    private void OnEnd(in BlackHoleMessage message)
    {
        if (!_active.TryRemove(message.Header, out Reassembly? state))
            return;

        try
        {
            state.Sink?.Flush();
            Completed?.Invoke(this, new StreamCompletedEventArgs(
                message.Header,
                state.Descriptor,
                state.Received,
                state.Buffer?.WrittenMemory ?? default,
                state.Buffer is not null));
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>Drops a stream in progress and raises <see cref="Aborted"/>.</summary>
    public void Abort(string streamId, string reason)
    {
        if (_active.TryRemove(streamId, out Reassembly? state))
            state.Dispose();
        Aborted?.Invoke(streamId, reason);
    }

    public void Dispose()
    {
        foreach (string key in _active.Keys)
        {
            if (_active.TryRemove(key, out Reassembly? state))
                state.Dispose();
        }
    }
}

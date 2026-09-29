using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net.Sockets;
using BigPipe.Client.Protocol;

namespace BigPipe.Client.Network;

/// <summary>
/// One pipelined TCP connection to a broker. Many requests may be in flight; responses are
/// matched by correlation id. Reads use <see cref="PipeReader"/> so frames are parsed without
/// intermediate copies of partial reads.
/// </summary>
internal sealed class BrokerConnection : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly PipeReader _reader;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<int, TaskCompletionSource<byte[]>> _pending = new();
    private readonly string _clientId;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _readLoop;
    private int _correlation;
    private volatile Exception? _failure;

    public string Endpoint { get; }

    private BrokerConnection(Socket socket, string clientId, string endpoint)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: true);
        _reader = PipeReader.Create(_stream, new StreamPipeReaderOptions(bufferSize: 64 * 1024, minimumReadSize: 16 * 1024));
        _clientId = clientId;
        Endpoint = endpoint;
        _readLoop = Task.Run(ReadLoopAsync);
    }

    public bool IsHealthy => _failure is null && _socket.Connected;

    public static async Task<BrokerConnection> ConnectAsync(string host, int port, string clientId, TimeSpan timeout, CancellationToken ct)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        socket.SendBufferSize = 1 << 20;
        socket.ReceiveBufferSize = 1 << 20;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            await socket.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            socket.Dispose();
            throw new BigPipeException(ErrorCode.ConnectionFailed, $"cannot connect to {host}:{port}: {e.Message}", e);
        }
        return new BrokerConnection(socket, clientId, $"{host}:{port}");
    }

    /// <summary>
    /// Sends a request and returns the response body (after the correlation id).
    /// When <paramref name="expectResponse"/> is false (acks=0 produce) returns an empty array.
    /// </summary>
    public async Task<byte[]> SendAsync(short apiKey, short version, Action<KafkaWriter> body, TimeSpan timeout, CancellationToken ct, bool expectResponse = true)
    {
        if (_failure is { } f)
            throw new BigPipeException(ErrorCode.ConnectionFailed, $"connection to {Endpoint} is broken: {f.Message}", f);
        var corr = Interlocked.Increment(ref _correlation);
        using var w = new KafkaWriter(512);
        w.Int32(0); // size placeholder
        w.Int16(apiKey);
        w.Int16(version);
        w.Int32(corr);
        w.String(_clientId);
        body(w);
        w.PatchInt32(0, w.Length - 4);

        TaskCompletionSource<byte[]>? tcs = null;
        if (expectResponse)
        {
            tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[corr] = tcs;
        }
        await _writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _stream.WriteAsync(w.WrittenMemory, ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _pending.TryRemove(corr, out _);
            Fail(e);
            throw new BigPipeException(ErrorCode.ConnectionFailed, $"write to {Endpoint} failed: {e.Message}", e);
        }
        finally
        {
            _writeLock.Release();
        }
        if (tcs is null) return [];
        using var reg = ct.Register(() => tcs.TrySetCanceled(ct));
        var done = await Task.WhenAny(tcs.Task, Task.Delay(timeout, CancellationToken.None)).ConfigureAwait(false);
        if (done != tcs.Task)
        {
            _pending.TryRemove(corr, out _);
            throw new BigPipeException(ErrorCode.Timeout, $"request {apiKey} v{version} to {Endpoint} timed out after {timeout.TotalMilliseconds:0} ms");
        }
        return await tcs.Task.ConfigureAwait(false);
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var result = await _reader.ReadAsync(_cts.Token).ConfigureAwait(false);
                var buffer = result.Buffer;
                while (TryReadFrame(ref buffer, out var corr, out var body))
                {
                    if (_pending.TryRemove(corr, out var tcs))
                        tcs.TrySetResult(body);
                }
                _reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) throw new IOException($"connection to {Endpoint} closed by broker");
            }
        }
        catch (Exception e)
        {
            Fail(e);
        }
    }

    private static bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out int correlation, out byte[] body)
    {
        correlation = 0;
        body = [];
        if (buffer.Length < 8) return false;
        Span<byte> head = stackalloc byte[8];
        buffer.Slice(0, 8).CopyTo(head);
        var size = BinaryPrimitives.ReadInt32BigEndian(head);
        if (buffer.Length < 4 + size) return false;
        correlation = BinaryPrimitives.ReadInt32BigEndian(head[4..]);
        // Own the body: decoded records reference it after the pipe buffer is recycled.
        body = buffer.Slice(8, size - 4).ToArray();
        buffer = buffer.Slice(4 + size);
        return true;
    }

    private void Fail(Exception e)
    {
        _failure ??= e;
        foreach (var kv in _pending)
        {
            if (_pending.TryRemove(kv.Key, out var tcs))
                tcs.TrySetException(new BigPipeException(ErrorCode.ConnectionFailed, $"connection to {Endpoint} lost: {e.Message}", e));
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        Fail(new ObjectDisposedException(nameof(BrokerConnection)));
        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch
        {
            // already closed
        }
        await _stream.DisposeAsync().ConfigureAwait(false);
        try
        {
            await _readLoop.ConfigureAwait(false);
        }
        catch
        {
            // read loop errors are already propagated to pending requests
        }
        _writeLock.Dispose();
        _cts.Dispose();
    }
}

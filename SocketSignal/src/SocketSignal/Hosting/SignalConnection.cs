// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using SocketSignal.Buffers;
using SocketSignal.Diagnostics;
using SocketSignal.Dispatch;
using SocketSignal.Protocol;

namespace SocketSignal.Hosting;

/// <summary>
/// One WebSocket, pumped. This is the engine underneath both <see cref="SocketSignalClient"/> and a
/// server-side <see cref="ClientConnection"/> - the two sides of the protocol are symmetric, so they
/// share the loop rather than each keeping their own copy of it.
/// </summary>
/// <remarks>
/// <para>The allocation story, which is the whole point of this class:</para>
/// <list type="bullet">
///   <item>One pooled receive buffer per connection, grown to fit and never released until close.</item>
///   <item>One pooled send buffer plus one reused <see cref="Utf8JsonWriter"/>, guarded by the send
///   lock, so encoding a frame writes UTF-8 straight into the socket buffer - no string, no
///   Encoding.GetBytes, no per-frame array.</item>
///   <item>Correlation ids are longs formatted into a stack buffer, not 32-character GUID strings.</item>
///   <item>Method dispatch reads the name off the receive buffer through <see cref="Utf8HandlerTable"/>.</item>
///   <item>Invocation state is pooled, so a call in flight costs a task and nothing else.</item>
/// </list>
/// </remarks>
internal sealed class SignalConnection : IAsyncDisposable
{
    private readonly WebSocket _socket;
    private readonly SocketSignalOptions _options;
    private readonly Utf8HandlerTable _handlers;
    private object? _sender;

    // -------- send path: one buffer, one writer, one lock --------
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly PooledBufferWriter _out;
    private readonly Utf8JsonWriter _writer;

    // -------- receive path --------
    private byte[] _receive;

    // -------- calls in flight --------
    private readonly ConcurrentDictionary<long, IPendingCall> _pending = new();
    private long _nextCallId;

    // -------- invocation pooling and backpressure --------
    private readonly ConcurrentQueue<Invocation> _invocationPool = new();
    private readonly SemaphoreSlim _invocationGate;

    private readonly CancellationTokenSource _lifetime = new();
    private int _closed;
    private long _lastActivityTicks = DateTime.UtcNow.Ticks;

    public SignalConnection(
        WebSocket socket, SocketSignalOptions options, Utf8HandlerTable handlers, object? sender)
    {
        _socket = socket;
        _options = options;
        _handlers = handlers;
        _sender = sender;
        _out = new PooledBufferWriter(options.ReceiveBufferSize);
        _writer = new Utf8JsonWriter(_out, new JsonWriterOptions { SkipValidation = true });
        _receive = ArrayPool<byte>.Shared.Rent(options.ReceiveBufferSize);
        _invocationGate = new SemaphoreSlim(options.MaxConcurrentInvocations, options.MaxConcurrentInvocations);
    }

    /// <summary>
    /// What handlers receive as their first parameter. The server sets this after construction
    /// because a ClientConnection needs its SignalConnection to exist first.
    /// </summary>
    public void SetSender(object sender) => _sender = sender;

    public SignalStatistics Statistics { get; } = new();

    /// <summary>UTC time of the last frame in either direction. Drives keepalive and idle eviction.</summary>
    public DateTime LastActivityUtc => new(Interlocked.Read(ref _lastActivityTicks), DateTimeKind.Utc);

    public bool IsOpen => _socket.State == WebSocketState.Open && _closed == 0;

    /// <summary>Raised once the pump has stopped, with the reason it stopped.</summary>
    public event Action<string>? Closed;

    /// <summary>Raised for a welcome frame, carrying the server-assigned client id.</summary>
    public event Action<string>? Welcomed;

    // =========================================================================================
    // Receive pump
    // =========================================================================================

    /// <summary>Reads frames until the socket closes, the token fires, or the peer misbehaves.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        CancellationToken token = linked.Token;
        string reason = "closed by peer";

        try
        {
            while (!token.IsCancellationRequested && _socket.State == WebSocketState.Open)
            {
                int length = await ReceiveFrameAsync(token).ConfigureAwait(false);
                if (length < 0) break;
                if (length == 0) continue;

                Touch();
                Statistics.OnReceived(length);
                await DispatchAsync(length, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            reason = "cancelled";
        }
        catch (WebSocketException ex)
        {
            reason = ex.WebSocketErrorCode.ToString();
        }
        catch (Exception ex)
        {
            reason = ex.Message;
        }
        finally
        {
            await CloseCoreAsync(reason).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads one whole WebSocket message into the pooled buffer. Returns its length, 0 for a frame
    /// worth skipping, or -1 when the peer closed. Grows the buffer in place for large messages.
    /// </summary>
    private async ValueTask<int> ReceiveFrameAsync(CancellationToken token)
    {
        int offset = 0;
        while (true)
        {
            if (offset == _receive.Length)
                Grow(offset + 1);

            ValueWebSocketReceiveResult result;
            try
            {
                result = await _socket.ReceiveAsync(_receive.AsMemory(offset), token).ConfigureAwait(false);
            }
            catch (WebSocketException) when (_closed != 0)
            {
                return -1;
            }

            if (result.MessageType == WebSocketMessageType.Close)
                return -1;

            offset += result.Count;
            if (offset > _options.MaxMessageSize)
                throw new SocketSignalException($"Frame exceeded MaxMessageSize ({_options.MaxMessageSize} bytes).");

            if (result.EndOfMessage)
                return offset;
        }
    }

    private void Grow(int required)
    {
        int size = Math.Min(Math.Max(required, _receive.Length * 2), _options.MaxMessageSize + 1);
        byte[] next = ArrayPool<byte>.Shared.Rent(size);
        _receive.AsSpan().CopyTo(next);
        ArrayPool<byte>.Shared.Return(_receive);
        _receive = next;
    }

    private ValueTask DispatchAsync(int length, CancellationToken token)
    {
        if (!SignalFrame.TryParse(_receive.AsSpan(0, length), out SignalFrame frame))
            return ValueTask.CompletedTask;

        switch (frame.Type)
        {
            case MessageType.Invoke:
                return BeginInvocationAsync(ref frame, token);

            case MessageType.Result:
                CompletePendingCall(ref frame);
                return ValueTask.CompletedTask;

            case MessageType.Ping:
                return SendPongAsync(frame.Id, token);

            case MessageType.Welcome:
                Welcomed?.Invoke(Encoding.UTF8.GetString(frame.Id));
                return ValueTask.CompletedTask;

            default:
                // Pong, and anything the peer invented: liveness was already recorded by Touch().
                return ValueTask.CompletedTask;
        }
    }

    // =========================================================================================
    // Inbound invocations
    // =========================================================================================

    private ValueTask BeginInvocationAsync(ref SignalFrame frame, CancellationToken token)
    {
        HandlerEntry? handler = _handlers.Find(frame.Method);
        if (handler is null)
        {
            if (!frame.ExpectReturn || frame.Id.IsEmpty)
                return ValueTask.CompletedTask;

            string missing = Encoding.UTF8.GetString(frame.Method);
            return SendErrorAsync(CopyId(frame.Id), frame.Id.Length, $"Method '{missing}' not found", token, pooledId: true);
        }

        Invocation invocation = RentInvocation(ref frame);
        return RunGatedAsync(handler, invocation, token);
    }

    /// <summary>
    /// Waits for a dispatch slot, then runs the handler off the pump. Holding the pump here - and
    /// only here - is the backpressure: a peer that floods faster than handlers drain stops being
    /// read, which pushes flow control back down to TCP.
    /// </summary>
    private async ValueTask RunGatedAsync(HandlerEntry handler, Invocation invocation, CancellationToken token)
    {
        await _invocationGate.WaitAsync(token).ConfigureAwait(false);
        _ = RunInvocationAsync(handler, invocation, token);
    }

    private async Task RunInvocationAsync(HandlerEntry handler, Invocation invocation, CancellationToken token)
    {
        try
        {
            ValueTask<object?> pending = handler.InvokeAsync(_sender, invocation.Args, _options.JsonOptions);
            object? result = await pending.ConfigureAwait(false);

            if (invocation.ExpectReturn)
                await SendResultAsync(invocation.Buffer, invocation.IdLength, result, token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (invocation.ExpectReturn)
            {
                try
                {
                    await SendErrorAsync(invocation.Buffer, invocation.IdLength, ex.Message, token).ConfigureAwait(false);
                }
                catch
                {
                    // The socket went away while reporting a handler fault. Nothing left to say.
                }
            }
        }
        finally
        {
            ReturnInvocation(invocation);
            _invocationGate.Release();
        }
    }

    // =========================================================================================
    // Outbound calls
    // =========================================================================================

    /// <summary>Fire and forget: sends an invoke and does not wait for a reply.</summary>
    public async ValueTask NotifyAsync(string method, object?[] args, CancellationToken token = default)
    {
        long id = Interlocked.Increment(ref _nextCallId);
        await _sendLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            Begin();
            SignalWriter.WriteInvoke(_writer, id, expectReturn: false, method, args, _options.JsonOptions);
            await FlushAsync(token).ConfigureAwait(false);
        }
        finally { _sendLock.Release(); }
    }

    /// <summary>Fire and forget with a single typed argument - no object[], no boxing.</summary>
    public async ValueTask NotifyAsync<TArg>(string method, TArg arg, CancellationToken token = default)
    {
        long id = Interlocked.Increment(ref _nextCallId);
        await _sendLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            Begin();
            SignalWriter.WriteInvokeSingle(_writer, id, expectReturn: false, method, arg, _options.JsonOptions);
            await FlushAsync(token).ConfigureAwait(false);
        }
        finally { _sendLock.Release(); }
    }

    /// <summary>Sends an invoke and waits for the matching result, honouring the call timeout.</summary>
    public async ValueTask<TResult?> CallAsync<TResult>(string method, object?[] args, CancellationToken token = default)
    {
        long id = Interlocked.Increment(ref _nextCallId);
        var pending = new PendingCall<TResult>(method);
        _pending[id] = pending;

        try
        {
            await _sendLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                Begin();
                SignalWriter.WriteInvoke(_writer, id, expectReturn: true, method, args, _options.JsonOptions);
                await FlushAsync(token).ConfigureAwait(false);
            }
            finally { _sendLock.Release(); }

            return await AwaitResultAsync(pending, method, token).ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    /// <summary>Single typed argument variant of the call above.</summary>
    public async ValueTask<TResult?> CallAsync<TArg, TResult>(string method, TArg arg, CancellationToken token = default)
    {
        long id = Interlocked.Increment(ref _nextCallId);
        var pending = new PendingCall<TResult>(method);
        _pending[id] = pending;

        try
        {
            await _sendLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                Begin();
                SignalWriter.WriteInvokeSingle(_writer, id, expectReturn: true, method, arg, _options.JsonOptions);
                await FlushAsync(token).ConfigureAwait(false);
            }
            finally { _sendLock.Release(); }

            return await AwaitResultAsync(pending, method, token).ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    private async ValueTask<TResult?> AwaitResultAsync<TResult>(PendingCall<TResult> pending, string method, CancellationToken token)
    {
        TimeSpan timeout = _options.CallTimeout;
        try
        {
            TResult? value = timeout == Timeout.InfiniteTimeSpan
                ? await pending.Task.WaitAsync(token).ConfigureAwait(false)
                : await pending.Task.WaitAsync(timeout, token).ConfigureAwait(false);
            Statistics.OnCallCompleted();
            return value;
        }
        catch (TimeoutException)
        {
            Statistics.OnCallFailed();
            throw new SignalTimeoutException(method, timeout);
        }
        catch
        {
            Statistics.OnCallFailed();
            throw;
        }
    }

    private void CompletePendingCall(ref SignalFrame frame)
    {
        if (frame.Id.IsEmpty || !TryParseId(frame.Id, out long id))
            return;
        if (!_pending.TryRemove(id, out IPendingCall? pending))
            return;

        if (frame.HasError)
            pending.Fail(Encoding.UTF8.GetString(frame.Error));
        else
            pending.Complete(frame.Result, _options.JsonOptions);
    }

    private static bool TryParseId(ReadOnlySpan<byte> id, out long value) =>
        System.Buffers.Text.Utf8Parser.TryParse(id, out value, out int consumed) && consumed == id.Length;

    // =========================================================================================
    // Send helpers. Every one encodes inside the send lock, into the reusable buffer.
    // =========================================================================================

    public async ValueTask SendWelcomeAsync(string clientId, string serverName, CancellationToken token)
    {
        await _sendLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            Begin();
            SignalWriter.WriteWelcome(_writer, clientId, serverName);
            await FlushAsync(token).ConfigureAwait(false);
        }
        finally { _sendLock.Release(); }
    }

    public async ValueTask SendPingAsync(CancellationToken token)
    {
        long id = Interlocked.Increment(ref _nextCallId);
        await _sendLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            Begin();
            SignalWriter.WritePing(_writer, id);
            await FlushAsync(token).ConfigureAwait(false);
        }
        finally { _sendLock.Release(); }
    }

    private ValueTask SendPongAsync(ReadOnlySpan<byte> id, CancellationToken token)
    {
        byte[] copy = CopyId(id);
        return SendPongCoreAsync(copy, id.Length, token);
    }

    private async ValueTask SendPongCoreAsync(byte[] idBuffer, int idLength, CancellationToken token)
    {
        try
        {
            await _sendLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                Begin();
                SignalWriter.WritePong(_writer, idBuffer.AsSpan(0, idLength));
                await FlushAsync(token).ConfigureAwait(false);
            }
            finally { _sendLock.Release(); }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(idBuffer);
        }
    }

    private async ValueTask SendResultAsync(byte[] idBuffer, int idLength, object? value, CancellationToken token)
    {
        await _sendLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            Begin();
            SignalWriter.WriteResult(_writer, idBuffer.AsSpan(0, idLength), value, _options.JsonOptions);
            await FlushAsync(token).ConfigureAwait(false);
        }
        finally { _sendLock.Release(); }
    }

    private async ValueTask SendErrorAsync(
        byte[] idBuffer, int idLength, string message, CancellationToken token, bool pooledId = false)
    {
        try
        {
            await _sendLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                Begin();
                SignalWriter.WriteError(_writer, idBuffer.AsSpan(0, idLength), message);
                await FlushAsync(token).ConfigureAwait(false);
            }
            finally { _sendLock.Release(); }
        }
        finally
        {
            if (pooledId) ArrayPool<byte>.Shared.Return(idBuffer);
        }
    }

    /// <summary>Rewinds the shared send buffer and writer. Only ever called under the send lock.</summary>
    private void Begin()
    {
        ObjectDisposedException.ThrowIf(_closed != 0, this);
        _out.Reset();
        _writer.Reset(_out);
    }

    private async ValueTask FlushAsync(CancellationToken token)
    {
        int length = _out.WrittenCount;
        await _socket.SendAsync(_out.WrittenMemory, WebSocketMessageType.Text, endOfMessage: true, token)
                     .ConfigureAwait(false);
        Statistics.OnSent(length);
        Touch();
    }

    private void Touch() => Interlocked.Exchange(ref _lastActivityTicks, DateTime.UtcNow.Ticks);

    // =========================================================================================
    // Invocation pooling
    // =========================================================================================

    /// <summary>
    /// Carries the parts of a frame that must outlive the receive buffer: the correlation id and
    /// the raw arguments, copied back to back into one rented array.
    /// </summary>
    private sealed class Invocation
    {
        public byte[] Buffer = [];
        public int IdLength;
        public int ArgsOffset;
        public int ArgsLength;
        public bool ExpectReturn;

        public ReadOnlySpan<byte> Args => Buffer.AsSpan(ArgsOffset, ArgsLength);
    }

    private Invocation RentInvocation(ref SignalFrame frame)
    {
        if (!_invocationPool.TryDequeue(out Invocation? invocation))
            invocation = new Invocation { Buffer = ArrayPool<byte>.Shared.Rent(256) };

        int needed = frame.Id.Length + frame.Args.Length;
        if (invocation.Buffer.Length < needed)
        {
            ArrayPool<byte>.Shared.Return(invocation.Buffer);
            invocation.Buffer = ArrayPool<byte>.Shared.Rent(needed);
        }

        frame.Id.CopyTo(invocation.Buffer);
        frame.Args.CopyTo(invocation.Buffer.AsSpan(frame.Id.Length));

        invocation.IdLength = frame.Id.Length;
        invocation.ArgsOffset = frame.Id.Length;
        invocation.ArgsLength = frame.Args.Length;
        invocation.ExpectReturn = frame.ExpectReturn;
        return invocation;
    }

    private void ReturnInvocation(Invocation invocation)
    {
        if (_invocationPool.Count < 64)
            _invocationPool.Enqueue(invocation);
        else
            ArrayPool<byte>.Shared.Return(invocation.Buffer);
    }

    private static byte[] CopyId(ReadOnlySpan<byte> id)
    {
        byte[] copy = ArrayPool<byte>.Shared.Rent(Math.Max(id.Length, 1));
        id.CopyTo(copy);
        return copy;
    }

    // =========================================================================================
    // Shutdown
    // =========================================================================================

    /// <summary>Closes the socket politely and fails everything still waiting on it.</summary>
    public async ValueTask CloseAsync(string reason = "closed by host")
    {
        await _lifetime.CancelAsync().ConfigureAwait(false);
        await CloseCoreAsync(reason).ConfigureAwait(false);
    }

    private async ValueTask CloseCoreAsync(string reason)
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;

        foreach (KeyValuePair<long, IPendingCall> entry in _pending)
        {
            if (_pending.TryRemove(entry.Key, out IPendingCall? pending))
                pending.Abort(reason);
        }

        try
        {
            if (_socket.State == WebSocketState.Open)
                await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None)
                             .ConfigureAwait(false);
        }
        catch
        {
            // The peer may already be gone. The close frame is a courtesy, not a requirement.
        }

        Closed?.Invoke(reason);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync("disposed").ConfigureAwait(false);

        _lifetime.Dispose();
        _writer.Dispose();
        _out.Dispose();
        _sendLock.Dispose();
        _invocationGate.Dispose();

        if (_receive.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_receive);
            _receive = [];
        }

        while (_invocationPool.TryDequeue(out Invocation? invocation))
            ArrayPool<byte>.Shared.Return(invocation.Buffer);

        _socket.Dispose();
    }
}

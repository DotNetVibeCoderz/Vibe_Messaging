// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Patterns;

/// <summary>One inbound call, as seen by a handler.</summary>
public readonly struct RpcRequest
{
    internal RpcRequest(ITransport transport, string method, ReadOnlyMemory<byte> payload)
    {
        Transport = transport;
        Method = method;
        Payload = payload;
    }

    /// <summary>The connection the call arrived on. Handlers may push messages back down it.</summary>
    public ITransport Transport { get; }

    /// <summary>Method name from the message header.</summary>
    public string Method { get; }

    /// <summary>
    /// Request body. Valid only until the handler's task completes - copy it if you keep it.
    /// </summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>Decodes the body as UTF-8.</summary>
    public string Text() => Payload.IsEmpty ? string.Empty : Encoding.UTF8.GetString(Payload.Span);
}

/// <summary>An asynchronous RPC implementation.</summary>
public delegate ValueTask<ReadOnlyMemory<byte>> RpcHandler(RpcRequest request, CancellationToken cancellationToken);

/// <summary>
/// Serves RPC calls: matches <see cref="MessageType.RpcRequest"/> against registered methods and
/// writes a reply carrying the same correlation id.
/// </summary>
/// <remarks>
/// Unlike v2 this always replies. A missing method or a throwing handler comes back as a response
/// flagged <see cref="MessageFlags.Error"/>, so the caller fails fast with
/// <see cref="RpcException"/> instead of hanging until its timeout.
/// </remarks>
public sealed class RpcServer
{
    private sealed record Registration(RpcHandler Handler, bool Detached);

    private readonly ConcurrentDictionary<string, Registration> _methods = new(StringComparer.Ordinal);

    /// <summary>Method names currently served.</summary>
    public ICollection<string> Methods => _methods.Keys;

    /// <summary>
    /// Registers an asynchronous handler. It runs on the connection's receive loop, so it may read
    /// <see cref="RpcRequest.Payload"/> in place - and must not wait on anything that needs another
    /// message to arrive on that same connection. Use <see cref="RegisterDetached"/> when it does.
    /// </summary>
    public RpcServer Register(string method, RpcHandler handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(method);
        ArgumentNullException.ThrowIfNull(handler);
        _methods[method] = new Registration(handler, Detached: false);
        return this;
    }

    /// <summary>
    /// Registers a handler that runs off the receive loop, so it may await anything - including an
    /// RPC call back to the very client that invoked it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A normal handler is awaited by the receive loop, which is what lets it read the payload
    /// without a copy. The cost is that the loop cannot deliver anything else while it runs: a
    /// handler that calls back to the same client would wait for a reply that only the blocked loop
    /// could hand it, and the connection would deadlock.
    /// </para>
    /// <para>
    /// A detached handler copies its payload up front and runs on the thread pool, so ordering
    /// against other messages on the connection is no longer guaranteed. Use it for the callback
    /// pattern and for genuinely long work; use <see cref="Register(string, RpcHandler)"/> for
    /// everything else.
    /// </para>
    /// </remarks>
    public RpcServer RegisterDetached(string method, RpcHandler handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(method);
        ArgumentNullException.ThrowIfNull(handler);
        _methods[method] = new Registration(handler, Detached: true);
        return this;
    }

    /// <summary>Registers a synchronous handler. The returned memory is written before the call returns, so returning the request payload is safe (that is how echo works).</summary>
    public RpcServer Register(string method, Func<RpcRequest, ReadOnlyMemory<byte>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Register(method, (request, _) => ValueTask.FromResult(handler(request)));
    }

    /// <summary>Registers a text-in, text-out handler.</summary>
    public RpcServer RegisterText(string method, Func<string, string> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Register(method, request => Encoding.UTF8.GetBytes(handler(request.Text())));
    }

    /// <summary>Stops serving a method.</summary>
    public bool Unregister(string method) => _methods.TryRemove(method, out _);

    /// <summary>Wires this server into a router.</summary>
    public RpcServer AttachTo(MessageRouter router)
    {
        ArgumentNullException.ThrowIfNull(router);
        router.On(MessageType.RpcRequest, HandleAsync);
        return this;
    }

    /// <summary>Handles one request. Assign directly to a router or a transport dispatcher.</summary>
    public async ValueTask HandleAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        if (message.Type != MessageType.RpcRequest)
            return;

        if (!_methods.TryGetValue(message.Header, out Registration? registration))
        {
            await ReplyErrorAsync(transport, message, $"Unknown method '{message.Header}'.", cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (registration.Detached)
        {
            // The payload has to be copied: the receive loop reclaims its buffer as soon as this
            // returns, and the handler now outlives that.
            BlackHoleMessage owned = message.ToOwned();
            _ = Task.Run(
                () => InvokeAsync(transport, owned, registration.Handler, cancellationToken).AsTask(),
                CancellationToken.None);
            return;
        }

        await InvokeAsync(transport, message, registration.Handler, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask InvokeAsync(
        ITransport transport, BlackHoleMessage message, RpcHandler handler, CancellationToken cancellationToken)
    {
        ReadOnlyMemory<byte> result;
        try
        {
            result = await handler(new RpcRequest(transport, message.Header, message.Payload), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            await ReplyErrorAsync(transport, message, $"{ex.GetType().Name}: {ex.Message}", cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if ((message.Flags & MessageFlags.NoReply) != 0)
            return;

        await transport.SendAsync(
            new BlackHoleMessage(MessageType.RpcResponse, message.Header, result, message.CorrelationId),
            cancellationToken).ConfigureAwait(false);
    }

    private static ValueTask ReplyErrorAsync(
        ITransport transport, in BlackHoleMessage request, string reason, CancellationToken cancellationToken) =>
        transport.SendAsync(
            new BlackHoleMessage(
                MessageType.RpcResponse,
                request.Header,
                Encoding.UTF8.GetBytes(reason),
                request.CorrelationId,
                MessageFlags.Error),
            cancellationToken);
}

/// <summary>
/// Calls remote methods and matches replies back to their callers.
/// </summary>
/// <remarks>
/// Correlation is an <see cref="Interlocked.Increment(ref long)"/> counter rather than v2's
/// <see cref="Guid"/>: 8 bytes instead of 16 on every request, and no cryptographic RNG call per
/// call site. Every pending call has a deadline, so a lost reply can no longer hang the caller
/// forever.
/// </remarks>
public sealed class RpcClient : IDisposable
{
    private sealed record Pending(string Method, TaskCompletionSource<byte[]> Completion, CancellationTokenRegistration Registration);

    private readonly ITransport _transport;
    private readonly ConcurrentDictionary<long, Pending> _pending = new();
    private long _nextCorrelationId;
    private bool _disposed;

    public RpcClient(ITransport transport)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _transport.Closed += OnTransportClosed;
    }

    /// <summary>Deadline applied when a call does not pass its own. Default 30 seconds.</summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Calls still waiting for a reply.</summary>
    public int PendingCalls => _pending.Count;

    /// <summary>Wires this client into a router.</summary>
    public RpcClient AttachTo(MessageRouter router)
    {
        ArgumentNullException.ThrowIfNull(router);
        router.On(MessageType.RpcResponse, HandleAsync);
        return this;
    }

    /// <summary>Completes the matching pending call. Assign to a router or a transport dispatcher.</summary>
    public ValueTask HandleAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        if (message.Type != MessageType.RpcResponse)
            return ValueTask.CompletedTask;

        if (!_pending.TryRemove(message.CorrelationId, out Pending? pending))
            return ValueTask.CompletedTask; // Late reply for a call that already timed out.

        pending.Registration.Dispose();

        if (message.IsError)
        {
            pending.Completion.TrySetException(new RpcException(pending.Method, message.PayloadAsString()));
        }
        else
        {
            // The payload lives in the transport's buffer and dies when this dispatch returns,
            // while the awaiting continuation runs later - so the copy here is mandatory.
            pending.Completion.TrySetResult(message.Payload.ToArray());
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Calls <paramref name="method"/> and waits for its reply.</summary>
    /// <exception cref="RpcException">The remote method failed, is unknown, or the deadline passed.</exception>
    public async Task<byte[]> CallAsync(
        string method,
        ReadOnlyMemory<byte> payload = default,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(method);
        ObjectDisposedException.ThrowIf(_disposed, this);

        long correlationId = Interlocked.Increment(ref _nextCorrelationId);
        var completion = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var deadline = new CancellationTokenSource(timeout ?? DefaultTimeout);
        // Linking costs a second source plus a registration, so only pay for it when the caller
        // actually brought a token. Most calls do not.
        using CancellationTokenSource? linked = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(deadline.Token, cancellationToken)
            : null;

        CancellationTokenRegistration registration = (linked ?? deadline).Token.Register(static state =>
        {
            var (client, id) = ((RpcClient, long))state!;
            if (client._pending.TryRemove(id, out Pending? pending))
                pending.Completion.TrySetException(
                    new RpcException(pending.Method, $"Call to '{pending.Method}' did not complete before its deadline."));
        }, (this, correlationId));

        _pending[correlationId] = new Pending(method, completion, registration);

        try
        {
            await _transport.SendAsync(
                new BlackHoleMessage(MessageType.RpcRequest, method, payload, correlationId),
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (_pending.TryRemove(correlationId, out Pending? failed))
                failed.Registration.Dispose();
            throw;
        }

        return await completion.Task.ConfigureAwait(false);
    }

    /// <summary>Text-in, text-out convenience wrapper.</summary>
    public async Task<string> CallTextAsync(
        string method, string payload, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        byte[] result = await CallAsync(method, Encoding.UTF8.GetBytes(payload), timeout, cancellationToken)
            .ConfigureAwait(false);
        return Encoding.UTF8.GetString(result);
    }

    /// <summary>Fire and forget: sends the request and never waits for a reply.</summary>
    public ValueTask NotifyAsync(string method, ReadOnlyMemory<byte> payload = default, CancellationToken cancellationToken = default) =>
        _transport.SendAsync(
            new BlackHoleMessage(MessageType.RpcRequest, method, payload, 0, MessageFlags.NoReply),
            cancellationToken);

    private void OnTransportClosed(ITransport transport, Exception? failure)
    {
        foreach (long key in _pending.Keys)
        {
            if (_pending.TryRemove(key, out Pending? pending))
            {
                pending.Registration.Dispose();
                pending.Completion.TrySetException(new RpcException(
                    pending.Method, failure?.Message ?? "The connection closed before the reply arrived."));
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _transport.Closed -= OnTransportClosed;
        OnTransportClosed(_transport, null);
    }
}

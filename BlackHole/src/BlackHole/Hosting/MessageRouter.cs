// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Hosting;

/// <summary>
/// Routes received messages to handlers by <see cref="MessageType"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is what replaced v2's "every pattern object subscribes to the same event and ignores what it
/// does not own" arrangement. Lookup is an array index on the type byte, the single-handler case
/// - which is every case in practice - forwards without allocating a state machine, and because
/// handlers return <see cref="ValueTask"/> the transport can await them and hold the receive buffer
/// steady while they run.
/// </para>
/// <para>
/// Registration is copy-on-write, so wiring up new handlers while traffic flows is safe.
/// </para>
/// </remarks>
public sealed class MessageRouter
{
    private readonly MessageDispatch[]?[] _handlers = new MessageDispatch[]?[256];
    private readonly Lock _registrationLock = new();

    /// <summary>Receives anything with no registered handler. Useful while developing.</summary>
    public MessageDispatch? Fallback { get; set; }

    /// <summary>Raised when a handler throws. Without a subscriber the exception is swallowed so one bad handler cannot kill the connection.</summary>
    public event Action<BlackHoleMessage, Exception>? HandlerFaulted;

    /// <summary>Registers a handler for one message type. Several handlers per type run in registration order.</summary>
    public MessageRouter On(MessageType type, MessageDispatch handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (_registrationLock)
        {
            MessageDispatch[]? existing = _handlers[(byte)type];
            _handlers[(byte)type] = existing is null ? [handler] : [.. existing, handler];
        }
        return this;
    }

    /// <summary>Registers the same handler for several types.</summary>
    public MessageRouter On(ReadOnlySpan<MessageType> types, MessageDispatch handler)
    {
        foreach (MessageType type in types)
            On(type, handler);
        return this;
    }

    /// <summary>Synchronous convenience wrapper for handlers that never need to await.</summary>
    public MessageRouter On(MessageType type, Action<ITransport, BlackHoleMessage> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return On(type, (transport, message, _) =>
        {
            handler(transport, message);
            return ValueTask.CompletedTask;
        });
    }

    /// <summary>Removes every handler for a type.</summary>
    public void Clear(MessageType type)
    {
        lock (_registrationLock)
            _handlers[(byte)type] = null;
    }

    /// <summary>Assign this to <see cref="ITransport.Dispatcher"/>.</summary>
    public MessageDispatch Dispatch => DispatchAsync;

    /// <summary>Routes one message. Exceptions from handlers surface through <see cref="HandlerFaulted"/>.</summary>
    public ValueTask DispatchAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        MessageDispatch[]? handlers = _handlers[(byte)message.Type];

        if (handlers is null || handlers.Length == 0)
        {
            MessageDispatch? fallback = Fallback;
            return fallback is null ? ValueTask.CompletedTask : Guarded(fallback, transport, message, cancellationToken);
        }

        // The overwhelmingly common shape: exactly one handler, forwarded with no extra machinery.
        if (handlers.Length == 1)
            return Guarded(handlers[0], transport, message, cancellationToken);

        return DispatchManyAsync(handlers, transport, message, cancellationToken);
    }

    private async ValueTask DispatchManyAsync(
        MessageDispatch[] handlers, ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        foreach (MessageDispatch handler in handlers)
            await Guarded(handler, transport, message, cancellationToken).ConfigureAwait(false);
    }

    private ValueTask Guarded(
        MessageDispatch handler, ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        try
        {
            ValueTask task = handler(transport, message, cancellationToken);
            return task.IsCompletedSuccessfully ? ValueTask.CompletedTask : Await(task, message);
        }
        catch (Exception ex)
        {
            HandlerFaulted?.Invoke(message, ex);
            return ValueTask.CompletedTask;
        }
    }

    private async ValueTask Await(ValueTask task, BlackHoleMessage message)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            HandlerFaulted?.Invoke(message, ex);
        }
    }
}

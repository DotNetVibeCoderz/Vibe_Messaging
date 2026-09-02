// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using SocketSignal.Diagnostics;

using SocketSignal.Hosting;

namespace SocketSignal;

/// <summary>
/// One connected client, as the server sees it. Handlers receive this, so a method can answer the
/// caller directly, call back into it, or stash per-connection state in <see cref="Items"/>.
/// </summary>
public sealed class ClientConnection
{
    private readonly SignalConnection _connection;
    private readonly SocketSignalServer _server;

    internal ClientConnection(string id, SignalConnection connection, SocketSignalServer server, IPEndPoint? remote)
    {
        Id = id;
        _connection = connection;
        _server = server;
        RemoteEndPoint = remote;
        ConnectedAtUtc = DateTime.UtcNow;
    }

    /// <summary>Server-assigned connection id. Sent to the client in the welcome frame.</summary>
    public string Id { get; }

    /// <summary>Where the client dialled in from, when the listener could tell.</summary>
    public IPEndPoint? RemoteEndPoint { get; }

    public DateTime ConnectedAtUtc { get; }

    /// <summary>Per-connection scratch space for application state - a user id, a session, a role.</summary>
    public ConcurrentDictionary<string, object?> Items { get; } = new();

    /// <summary>Frame and byte counters for this connection alone.</summary>
    public SignalStatistics Statistics => _connection.Statistics;

    public bool IsOpen => _connection.IsOpen;

    /// <summary>Groups this connection currently belongs to.</summary>
    public IReadOnlyCollection<string> Groups => _server.GroupsOf(Id);

    /// <summary>Calls a method on this client without waiting for a reply.</summary>
    public ValueTask SendAsync(string method, params object?[] args) =>
        _connection.NotifyAsync(method, args);

    /// <summary>Calls a method on this client with one typed argument - the allocation-free path.</summary>
    public ValueTask SendAsync<TArg>(string method, TArg arg) =>
        _connection.NotifyAsync(method, arg);

    /// <summary>
    /// Calls a method on this client and waits for its return value.
    /// </summary>
    /// <remarks>
    /// In v1 the machinery for this existed but was <c>internal</c>, so server-to-client calls could
    /// never actually return anything. This is that feature, finished.
    /// </remarks>
    public ValueTask<TResult?> CallAsync<TResult>(string method, params object?[] args) =>
        _connection.CallAsync<TResult>(method, args);

    /// <inheritdoc cref="CallAsync{TResult}(string, object?[])"/>
    public ValueTask<JsonElement?> CallAsync(string method, params object?[] args) =>
        _connection.CallAsync<JsonElement?>(method, args);

    /// <summary>Adds this connection to a group. Groups are created on first use.</summary>
    public void JoinGroup(string groupName) => _server.AddToGroup(groupName, Id);

    /// <summary>Removes this connection from a group.</summary>
    public void LeaveGroup(string groupName) => _server.RemoveFromGroup(groupName, Id);

    /// <summary>Closes this connection and fails anything still waiting on it.</summary>
    public ValueTask CloseAsync(string reason = "closed by server") => _connection.CloseAsync(reason);

    internal SignalConnection Connection => _connection;

    public override string ToString() => $"client {Id} from {RemoteEndPoint?.ToString() ?? "unknown"}";
}

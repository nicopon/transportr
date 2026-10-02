using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

namespace TransportR.Hub.SignalR.Access;

/// <summary>
/// The connections this hub instance holds that have registered a client, so that one whose token
/// ran out can be closed from outside a hub method.
/// </summary>
/// <remarks>
/// Filled by <c>CommandHub.Connect</c>, which a derived hub cannot override, and emptied when the
/// connection goes. A connection that never registered holds no right to judge. Local to the
/// instance: another instance closes the connections it holds.
/// </remarks>
public sealed class ConnectionRegistry
{
    private readonly ConcurrentDictionary<string, HubCallerContext> _connections = new();

    internal void Add(HubCallerContext context) => _connections[context.ConnectionId] = context;

    internal void Remove(string connectionId) => _connections.TryRemove(connectionId, out _);

    /// <returns>True when this instance holds the connection and has told it to close.</returns>
    internal bool TryClose(string connectionId)
    {
        if (!_connections.TryGetValue(connectionId, out var context)) return false;

        context.Abort();
        return true;
    }
}

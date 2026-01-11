using System.Collections.Concurrent;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Hub.SignalR.Services;

/// <summary>
/// Manages connected clients, their groups, and token expiration.
/// </summary>
public class ClientManager : IClientManager
{
    // ClientId -> ConnectionId
    private readonly ConcurrentDictionary<Guid, string> _clientConnections = new();
    
    // ClientId -> Groups (multi-group support)
    private readonly ConcurrentDictionary<Guid, HashSet<string>> _clientGroups = new();
    
    // ClientId -> TokenExpiration (UTC)
    private readonly ConcurrentDictionary<Guid, DateTime> _clientTokenExpiry = new();

    /// <summary>
    /// Registers a client with its groups and optional token expiration.
    /// </summary>
    public void RegisterClient(Guid clientId, string connectionId, IEnumerable<string> groups, DateTime? tokenExpiry = null)
    {
        _clientConnections[clientId] = connectionId;
        _clientGroups[clientId] = groups.ToHashSet();
        if (tokenExpiry.HasValue)
        {
            _clientTokenExpiry[clientId] = tokenExpiry.Value;
        }
    }
    
    /// <summary>
    /// Updates the token expiration for a client (called during heartbeat).
    /// </summary>
    public void UpdateTokenExpiry(Guid clientId, DateTime expiry)
    {
        _clientTokenExpiry[clientId] = expiry;
    }
    
    /// <summary>
    /// Checks if a client's token has expired.
    /// </summary>
    public bool IsTokenExpired(Guid clientId)
    {
        if (!_clientTokenExpiry.TryGetValue(clientId, out var expiry))
            return false; // No expiry tracked = not expired (unauthenticated mode)
        
        return DateTime.UtcNow > expiry;
    }
    
    /// <summary>
    /// Gets the token expiration for a client.
    /// </summary>
    public DateTime? GetTokenExpiry(Guid clientId)
    {
        return _clientTokenExpiry.TryGetValue(clientId, out var expiry) ? expiry : null;
    }

    /// <summary>
    /// Removes a client by its connection ID.
    /// </summary>
    public void RemoveClient(string connectionId)
    {
        var item = _clientConnections.FirstOrDefault(kvp => kvp.Value == connectionId);
        if (item.Key != Guid.Empty)
        {
            _clientConnections.TryRemove(item.Key, out _);
            _clientGroups.TryRemove(item.Key, out _);
            _clientTokenExpiry.TryRemove(item.Key, out _);
        }
    }

    /// <summary>
    /// Gets the connection ID for a client.
    /// </summary>
    public string? GetConnectionId(Guid clientId)
    {
        return _clientConnections.TryGetValue(clientId, out var connectionId) ? connectionId : null;
    }

    /// <summary>
    /// Gets the client ID for a connection.
    /// </summary>
    public Guid GetClientId(string connectionId)
    {
        return _clientConnections.FirstOrDefault(x => x.Value == connectionId).Key;
    }

    /// <summary>
    /// Gets the groups for a client.
    /// </summary>
    public IReadOnlySet<string> GetClientGroups(Guid clientId)
    {
        return _clientGroups.TryGetValue(clientId, out var groups) ? groups : new HashSet<string>();
    }

    /// <summary>
    /// Gets all connected clients.
    /// </summary>
    public List<ClientInfo> GetAllClients()
    {
        return _clientGroups
            .Select(kvp => new ClientInfo
            {
                ClientId = kvp.Key,
                Groups = kvp.Value
            }).ToList();
    }
}

using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Interface for managing connected clients and their group memberships.
/// </summary>
public interface IClientManager
{
    /// <summary>
    /// Registers a client with its groups and optional token expiration.
    /// </summary>
    void RegisterClient(Guid clientId, string connectionId, IEnumerable<string> groups, DateTime? tokenExpiry = null);
    
    /// <summary>
    /// Updates the token expiration for a client.
    /// </summary>
    void UpdateTokenExpiry(Guid clientId, DateTime expiry);
    
    /// <summary>
    /// Checks if a client's token has expired.
    /// </summary>
    bool IsTokenExpired(Guid clientId);
    
    /// <summary>
    /// Gets the token expiration for a client.
    /// </summary>
    DateTime? GetTokenExpiry(Guid clientId);

    /// <summary>
    /// Removes a client by its connection ID.
    /// </summary>
    void RemoveClient(string connectionId);

    /// <summary>
    /// Gets the connection ID for a client.
    /// </summary>
    string? GetConnectionId(Guid clientId);

    /// <summary>
    /// Gets the client ID for a connection.
    /// </summary>
    Guid GetClientId(string connectionId);

    /// <summary>
    /// Gets the groups for a client.
    /// </summary>
    IReadOnlySet<string> GetClientGroups(Guid clientId);

    /// <summary>
    /// Gets all connected clients.
    /// </summary>
    List<ClientInfo> GetAllClients();
}

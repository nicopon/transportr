using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Interface for persisting hub state (clients, checkpoints).
/// Implementations: InMemory, TestProxy, Redis.
/// </summary>
public interface IStateStore
{
    // ========== Clients ==========
    
    /// <summary>
    /// Registers a client in the shared store.
    /// </summary>
    Task SaveClientAsync(ClientRegistration client);
    
    /// <summary>
    /// Gets a client by ID.
    /// </summary>
    Task<ClientRegistration?> GetClientAsync(Guid clientId);
    
    /// <summary>
    /// Gets a client by SignalR connection ID.
    /// More efficient than GetAllClientsAsync for single lookups.
    /// </summary>
    Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId);
    
    /// <summary>
    /// Gets all connected clients.
    /// </summary>
    Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync();
    
    /// <summary>
    /// Records that the connection identified by <paramref name="connectionId"/> is gone, and does
    /// nothing if the registration has since moved to another connection.
    /// </summary>
    /// <remarks>
    /// A disconnect and the reconnect that replaces it race by nature. Read-modify-write through
    /// <see cref="SaveClientAsync"/> loses that race: the departing connection writes back the
    /// registration it read, reinstating its own dead ConnectionId over the live one. Marking is
    /// therefore the store's job, not the caller's — only the store can make the check and the
    /// write one step.
    /// </remarks>
    Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc);

    /// <summary>
    /// Removes a client from the store.
    /// </summary>
    Task DeleteClientAsync(Guid clientId);
    
    /// <summary>
    /// Updates token expiry for a client.
    /// </summary>
    Task UpdateClientTokenExpiryAsync(Guid clientId, DateTime expiry);

    // ========== Transfers (HA Sessions) ==========

    /// <summary>
    /// Saves a transfer session for HA routing.
    /// Expiry should be handled by the implementation based on session.ExpiresAtUtc.
    /// </summary>
    Task SaveSessionAsync(TransferSession session);

    /// <summary>
    /// Gets a transfer session if valid (not expired).
    /// </summary>
    Task<TransferSession?> GetSessionAsync(string transferId);

    /// <summary>
    /// Extends the expiry of a session (Heartbeat).
    /// </summary>
    Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry);

    /// <summary>
    /// Deletes a session explicitly (Completion/Cancellation).
    /// </summary>
    Task DeleteSessionAsync(string transferId);
}

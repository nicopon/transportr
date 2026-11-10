using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Interface for persisting hub state (clients, transfer sessions).
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
    /// Gets a client by the id of the event stream it registered on.
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
    /// write one step. A write that lost to <see cref="RenewClientAsync"/> reads again: the
    /// connection is still the registration's, and still gone.
    /// </remarks>
    Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc);

    /// <summary>
    /// Removes a client from the store.
    /// </summary>
    Task DeleteClientAsync(Guid clientId);

    /// <summary>
    /// Replaces the groups and the token expiry of a registration, and nothing else, provided it
    /// still belongs to <paramref name="expectedSubject"/>.
    /// </summary>
    /// <returns>
    /// The registration as written, or <c>null</c> when there is none or it belongs to another
    /// subject. Two absent subjects are the same subject.
    /// </returns>
    /// <remarks>
    /// The connection and the disconnection mark stay as they are: a renewal is a request of its own,
    /// not part of the client's stream, and says nothing of whether that is still up. Like
    /// <see cref="MarkDisconnectedAsync"/> it must be one step with its check, or it writes back a
    /// registration that a reconnect has replaced since it was read.
    /// </remarks>
    Task<ClientRegistration?> RenewClientAsync(Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry);

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
    /// <remarks>
    /// A session that is absent stays absent. The heartbeat of a transfer races its removal, so an
    /// extension that reads the session and writes it back must be one step with its read, or it
    /// brings back a session deleted in between, and the ended transfer with it.
    /// </remarks>
    Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry);

    /// <summary>
    /// Deletes a session explicitly (Completion/Cancellation).
    /// </summary>
    Task DeleteSessionAsync(string transferId);
}

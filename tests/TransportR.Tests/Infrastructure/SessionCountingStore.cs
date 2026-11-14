using TransportR.Hub;

namespace TransportR.Tests.Infrastructure;

/// <summary>An in-memory store that counts the sessions written to it, refused or not by what follows.</summary>
public sealed class SessionCountingStore : IStateStore
{
    private readonly InMemoryStateStore _inner = new();
    private int _sessionsSaved;

    public int SessionsSaved => Volatile.Read(ref _sessionsSaved);

    public Task SaveSessionAsync(TransferSession session)
    {
        Interlocked.Increment(ref _sessionsSaved);
        return _inner.SaveSessionAsync(session);
    }

    public Task<TransferSession?> GetSessionAsync(string transferId) => _inner.GetSessionAsync(transferId);
    public Task<int> CountTransfersOpenedByAsync(Guid senderId) => _inner.CountTransfersOpenedByAsync(senderId);
    public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry) => _inner.ExtendSessionExpiryAsync(transferId, newExpiry);
    public Task DeleteSessionAsync(string transferId) => _inner.DeleteSessionAsync(transferId);
    public Task SaveClientAsync(ClientRegistration client) => _inner.SaveClientAsync(client);
    public Task<ClientRegistration?> RenewClientAsync(Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry) =>
        _inner.RenewClientAsync(clientId, expectedSubject, groups, tokenExpiry);
    public Task<ClientRegistration?> GetClientAsync(Guid clientId) => _inner.GetClientAsync(clientId);
    public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId) => _inner.GetClientByConnectionIdAsync(connectionId);
    public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync() => _inner.GetAllClientsAsync();
    public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc) => _inner.MarkDisconnectedAsync(connectionId, whenUtc);
    public Task DeleteClientAsync(Guid clientId) => _inner.DeleteClientAsync(clientId);
}

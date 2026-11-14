using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace TransportR.Hub;

/// <summary>
/// In-memory implementation of IStateStore.
/// For single-instance deployment or development.
/// </summary>
public class InMemoryStateStore : IStateStore
{
    private readonly ConcurrentDictionary<Guid, ClientRegistration> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _connectionIndex = new(); // connectionId -> clientId
    private readonly ConcurrentDictionary<string, TransferSession> _sessions = new();

    private readonly TimeSpan _disconnectedGrace;
    private readonly TimeProvider _time;

    public InMemoryStateStore(IOptions<ClientRegistryConfiguration>? config = null, TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        var settings = config?.Value ?? new ClientRegistryConfiguration();
        _disconnectedGrace = TimeSpan.FromSeconds(settings.DisconnectedGraceSeconds);
    }

    private DateTime Now => _time.GetUtcNow().UtcDateTime;

    // ========== Clients ==========

    public Task SaveClientAsync(ClientRegistration client)
    {
        // A reconnecting client comes back under a new ConnectionId. Drop the index entry pointing
        // at the old one, or it accumulates for the lifetime of the process and keeps resolving to
        // a connection that is gone.
        if (_clients.TryGetValue(client.ClientId, out var previous) &&
            previous.ConnectionId != client.ConnectionId)
        {
            _connectionIndex.TryRemove(previous.ConnectionId, out _);
        }

        _clients[client.ClientId] = client;
        _connectionIndex[client.ConnectionId] = client.ClientId;
        return Task.CompletedTask;
    }

    public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc)
    {
        while (_connectionIndex.TryGetValue(connectionId, out var clientId) &&
               _clients.TryGetValue(clientId, out var current) &&
               current.ConnectionId == connectionId)
        {
            // Compare-and-swap on the registration read above. A Connect that landed in between
            // moved it to another connection, and the loop ends there rather than reinstating the
            // ConnectionId that just died; a renewal changed it under the same connection, and the
            // mark is still owed.
            if (_clients.TryUpdate(clientId, current with { DisconnectedAtUtc = whenUtc }, current)) break;
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Evicts a registration whose connection dropped more than the grace period ago, the way
    /// sessions are expired lazily below. Without this a registration lives for the lifetime of
    /// the process: nothing calls <see cref="DeleteClientAsync"/> on a disconnect, by design.
    /// </summary>
    private bool IsExpired(ClientRegistration client)
    {
        if (client.DisconnectedAtUtc is not { } disconnectedAt) return false;
        if (Now - disconnectedAt <= _disconnectedGrace) return false;

        _clients.TryRemove(client.ClientId, out _);
        _connectionIndex.TryRemove(client.ConnectionId, out _);
        return true;
    }

    public Task<ClientRegistration?> GetClientAsync(Guid clientId)
    {
        if (_clients.TryGetValue(clientId, out var client) && !IsExpired(client))
        {
            return Task.FromResult<ClientRegistration?>(client);
        }
        return Task.FromResult<ClientRegistration?>(null);
    }

    public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
    {
        if (_connectionIndex.TryGetValue(connectionId, out var clientId) &&
            _clients.TryGetValue(clientId, out var client) &&
            !IsExpired(client))
        {
            return Task.FromResult<ClientRegistration?>(client);
        }
        return Task.FromResult<ClientRegistration?>(null);
    }

    public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync()
    {
        var live = _clients.Values.Where(c => !IsExpired(c)).ToList();
        return Task.FromResult<IReadOnlyList<ClientRegistration>>(live);
    }

    public Task DeleteClientAsync(Guid clientId)
    {
        if (_clients.TryRemove(clientId, out var client))
        {
            _connectionIndex.TryRemove(client.ConnectionId, out _);
        }
        return Task.CompletedTask;
    }

    public Task<ClientRegistration?> RenewClientAsync(
        Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry)
    {
        while (_clients.TryGetValue(clientId, out var current) && !IsExpired(current))
        {
            if (!string.Equals(current.Subject, expectedSubject, StringComparison.Ordinal))
            {
                return Task.FromResult<ClientRegistration?>(null);
            }

            var renewed = current with { Groups = groups.ToHashSet(), TokenExpiry = tokenExpiry };
            if (_clients.TryUpdate(clientId, renewed, current))
            {
                return Task.FromResult<ClientRegistration?>(renewed);
            }
        }

        return Task.FromResult<ClientRegistration?>(null);
    }

    // ========== Transfers (HA Sessions) ==========

    public Task SaveSessionAsync(TransferSession session)
    {
        _sessions[session.TransferId] = session;
        return Task.CompletedTask;
    }

    public Task<TransferSession?> GetSessionAsync(string transferId)
    {
        if (_sessions.TryGetValue(transferId, out var session))
        {
            if (session.ExpiresAtUtc > Now)
                return Task.FromResult<TransferSession?>(session);

            // Lazy expiration, of the value read only: an extension that landed since is kept.
            _sessions.TryRemove(new KeyValuePair<string, TransferSession>(transferId, session));
        }
        return Task.FromResult<TransferSession?>(null);
    }

    public Task<int> CountTransfersOpenedByAsync(Guid senderId)
    {
        var now = Now;
        var count = _sessions.Count(entry =>
            entry.Value.OpenedBySender && entry.Value.SenderId == senderId && entry.Value.ExpiresAtUtc > now);
        return Task.FromResult(count);
    }

    public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry)
    {
        while (_sessions.TryGetValue(transferId, out var session) && session.ExpiresAtUtc > Now)
        {
            // Compare-and-swap on the session read above: a delete that landed in between ends the
            // loop, where a plain write would bring the ended transfer back for a whole TTL.
            if (_sessions.TryUpdate(transferId, session with { ExpiresAtUtc = newExpiry }, session)) break;
        }

        return Task.CompletedTask;
    }

    public Task DeleteSessionAsync(string transferId)
    {
        _sessions.TryRemove(transferId, out _);
        return Task.CompletedTask;
    }
}

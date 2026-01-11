using System.Collections.Concurrent;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// In-memory implementation of IStateStore.
/// For single-instance deployment or development.
/// </summary>
public class InMemoryStateStore : IStateStore
{
    private readonly ConcurrentDictionary<Guid, ClientRegistration> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _connectionIndex = new(); // connectionId -> clientId
    private readonly ConcurrentDictionary<string, TransferSession> _sessions = new(); 

    // ========== Clients ==========

    public Task SaveClientAsync(ClientRegistration client)
    {
        _clients[client.ClientId] = client;
        _connectionIndex[client.ConnectionId] = client.ClientId;
        return Task.CompletedTask;
    }

    public Task<ClientRegistration?> GetClientAsync(Guid clientId)
    {
        return Task.FromResult(_clients.TryGetValue(clientId, out var client) ? client : null);
    }

    public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
    {
        if (_connectionIndex.TryGetValue(connectionId, out var clientId) &&
            _clients.TryGetValue(clientId, out var client))
        {
            return Task.FromResult<ClientRegistration?>(client);
        }
        return Task.FromResult<ClientRegistration?>(null);
    }

    public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync()
    {
        return Task.FromResult<IReadOnlyList<ClientRegistration>>(_clients.Values.ToList());
    }

    public Task DeleteClientAsync(Guid clientId)
    {
        if (_clients.TryRemove(clientId, out var client))
        {
            _connectionIndex.TryRemove(client.ConnectionId, out _);
        }
        return Task.CompletedTask;
    }

    public Task UpdateClientTokenExpiryAsync(Guid clientId, DateTime expiry)
    {
        if (_clients.TryGetValue(clientId, out var existing))
        {
            _clients[clientId] = existing with { TokenExpiry = expiry };
        }
        return Task.CompletedTask;
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
            if (session.ExpiresAtUtc > DateTime.UtcNow)
                return Task.FromResult<TransferSession?>(session);
            
            // Lazy expiration
            _sessions.TryRemove(transferId, out _);
        }
        return Task.FromResult<TransferSession?>(null);
    }

    public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry)
    {
        if (_sessions.TryGetValue(transferId, out var session))
        {
            if (session.ExpiresAtUtc > DateTime.UtcNow)
            {
                _sessions[transferId] = session with { ExpiresAtUtc = newExpiry };
            }
        }
        return Task.CompletedTask;
    }

    public Task DeleteSessionAsync(string transferId)
    {
        _sessions.TryRemove(transferId, out _);
        return Task.CompletedTask;
    }
}

using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Storage.Redis;

/// <summary>
/// IStateStore implementation using Redis.
/// Handles distributed state with TTL for clients and checkpoints.
/// </summary>
public class RedisStateStore : IStateStore
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisStateStore> _logger;
    private readonly IDatabase _db;

    // Keys and TTLs
    private const string KeyPrefix = "transportr:";
    private static readonly TimeSpan ClientTtl = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan CheckpointTtl = TimeSpan.FromHours(24);

    public RedisStateStore(IConnectionMultiplexer redis, ILogger<RedisStateStore> logger)
    {
        _redis = redis;
        _logger = logger;
        _db = _redis.GetDatabase();
    }

    // ========== Transfers (HA Sessions) ==========

    public async Task SaveSessionAsync(TransferSession session)
    {
        var key = $"{KeyPrefix}sessions:{session.TransferId}";
        var json = JsonSerializer.Serialize(session);
        var ttl = session.ExpiresAtUtc - DateTime.UtcNow;
        
        if (ttl <= TimeSpan.Zero)
        {
             // Already expired - do not save, and ensure it is removed if it existed
             await _db.KeyDeleteAsync(key);
             return;
        } 
        
        await _db.StringSetAsync(key, json, ttl);
    }

    public async Task<TransferSession?> GetSessionAsync(string transferId)
    {
        var key = $"{KeyPrefix}sessions:{transferId}";
        var json = await _db.StringGetAsync(key);

        if (json.IsNullOrEmpty) return null;

        return JsonSerializer.Deserialize<TransferSession>(json.ToString()!);
    }

    public async Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry)
    {
        var key = $"{KeyPrefix}sessions:{transferId}";
        var ttl = newExpiry - DateTime.UtcNow;
        
        if (ttl <= TimeSpan.Zero)
        {
             // If expired, ensure it's deleted
             await _db.KeyDeleteAsync(key);
             return;
        }

        // 1. Update TTL
        await _db.KeyExpireAsync(key, ttl);
        
        // 2. We also need to update the payload's ExpiresAtUtc if we want GetSession to be accurate.
        // However, fetching+deserializing+serializing+saving is expensive for a heartbeat.
        // DECISION: We blindly extend TTL. GetSession might show an old ExpiresAtUtc but the key is alive.
        // If exact consistency is required, we should use a Lua script or just accept the trade-off.
        // Given this is for Liveness, TTL is what counts.
    }

    public async Task DeleteSessionAsync(string transferId)
    {
        var key = $"{KeyPrefix}sessions:{transferId}";
        await _db.KeyDeleteAsync(key);
    }

    // --- Clients ---

    public async Task SaveClientAsync(ClientRegistration client)
    {
        var key = $"{KeyPrefix}clients:{client.ClientId}";
        var connKey = $"{KeyPrefix}conn:{client.ConnectionId}";
        var json = JsonSerializer.Serialize(client);
        
        await _db.StringSetAsync(key, json, ClientTtl);
        
        // Secondary index: connectionId -> clientId (for O(1) lookup)
        await _db.StringSetAsync(connKey, client.ClientId.ToString(), ClientTtl);
        
        // Also add to a set for GetAllClients enumeration
        await _db.SetAddAsync($"{KeyPrefix}client_ids", client.ClientId.ToString());
    }

    public async Task<ClientRegistration?> GetClientAsync(Guid clientId)
    {
        var key = $"{KeyPrefix}clients:{clientId}";
        var json = await _db.StringGetAsync(key);

        if (json.IsNullOrEmpty) return null;

        return JsonSerializer.Deserialize<ClientRegistration>(json.ToString()!);
    }

    public async Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
    {
        // 1. Lookup clientId from connectionId index
        var connKey = $"{KeyPrefix}conn:{connectionId}";
        var clientIdStr = await _db.StringGetAsync(connKey);
        
        if (clientIdStr.IsNullOrEmpty) return null;
        if (!Guid.TryParse(clientIdStr.ToString(), out var clientId)) return null;
        
        // 2. Get client by ID
        return await GetClientAsync(clientId);
    }

    public async Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync()
    {
        // 1. Get all IDs
        var ids = await _db.SetMembersAsync($"{KeyPrefix}client_ids");
        if (ids.Length == 0) return Array.Empty<ClientRegistration>();

        var tasks = new List<Task<RedisValue>>();
        var keys = new RedisKey[ids.Length];
        
        for (int i = 0; i < ids.Length; i++)
        {
            keys[i] = $"{KeyPrefix}clients:{ids[i]}";
        }

        // 2. MGET all clients efficiently
        var values = await _db.StringGetAsync(keys);
        
        var clients = new List<ClientRegistration>();
        var missingIds = new List<RedisValue>();

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].HasValue)
            {
                var client = JsonSerializer.Deserialize<ClientRegistration>(values[i].ToString()!);
                if (client != null) clients.Add(client);
            }
            else
            {
                // Expired client, cleanup set
                missingIds.Add(ids[i]);
            }
        }

        // 3. Lazy cleanup of expired IDs from the set
        if (missingIds.Count > 0)
        {
            await _db.SetRemoveAsync($"{KeyPrefix}client_ids", missingIds.ToArray());
        }

        return clients;
    }

    public async Task DeleteClientAsync(Guid clientId)
    {
        // Get client first to cleanup connection index
        var client = await GetClientAsync(clientId);
        
        var key = $"{KeyPrefix}clients:{clientId}";
        await _db.KeyDeleteAsync(key);
        await _db.SetRemoveAsync($"{KeyPrefix}client_ids", clientId.ToString());
        
        // Cleanup connection index
        if (client != null)
        {
            var connKey = $"{KeyPrefix}conn:{client.ConnectionId}";
            await _db.KeyDeleteAsync(connKey);
        }
    }

    public async Task UpdateClientTokenExpiryAsync(Guid clientId, DateTime expiry)
    {
        var client = await GetClientAsync(clientId);
        if (client != null)
        {
            var updated = client with { TokenExpiry = expiry };
            await SaveClientAsync(updated);
        }
    }

}

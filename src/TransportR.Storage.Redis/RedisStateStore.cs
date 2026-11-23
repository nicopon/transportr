using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using TransportR.Hub;

namespace TransportR.Storage.Redis;

/// <summary>
/// IStateStore implementation using Redis.
/// Handles distributed state with TTL for clients and transfer sessions.
/// </summary>
public class RedisStateStore : IStateStore, IDisposable
{
    private readonly IConnectionMultiplexer _redis;
    private readonly bool _ownsConnection;
    private readonly IDatabase _db;
    private readonly string _keyPrefix;

    // A conditional write that keeps losing to a concurrent one is not the store's to settle: it
    // says so, and the caller's own retry (a client's next renewal) decides.
    private const int MaxWriteAttempts = 5;

    // A connected registration and a dropped one are not kept for the same reason, so they do not
    // get the same TTL: one short enough for a dropped client would remove a client that is merely
    // idle between transfers from discovery while it is still connected.
    private readonly TimeSpan _connectedTtl;
    private readonly TimeSpan _disconnectedGrace;
    private readonly TimeProvider _time;

    public RedisStateStore(
        IConnectionMultiplexer redis,
        IOptions<ClientRegistryConfiguration>? config = null,
        TimeProvider? timeProvider = null,
        IOptions<RedisStateStoreOptions>? storeOptions = null)
        : this(redis, ownsConnection: false, config, timeProvider, storeOptions)
    {
    }

    internal RedisStateStore(
        IConnectionMultiplexer redis,
        bool ownsConnection,
        IOptions<ClientRegistryConfiguration>? config = null,
        TimeProvider? timeProvider = null,
        IOptions<RedisStateStoreOptions>? storeOptions = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _ownsConnection = ownsConnection;
        _db = _redis.GetDatabase();
        _keyPrefix = storeOptions is null
            ? "transportr:"
            : storeOptions.Value.KeyPrefix ?? throw new ArgumentNullException(nameof(storeOptions), "RedisStateStoreOptions.KeyPrefix cannot be null.");

        var settings = config?.Value ?? new ClientRegistryConfiguration();
        _connectedTtl = TimeSpan.FromSeconds(settings.ConnectedTtlSeconds);
        _disconnectedGrace = TimeSpan.FromSeconds(settings.DisconnectedGraceSeconds);
    }

    public void Dispose()
    {
        if (_ownsConnection)
        {
            _redis.Dispose();
        }
    }

    private TimeSpan TtlFor(ClientRegistration client) =>
        client.DisconnectedAtUtc.HasValue ? _disconnectedGrace : _connectedTtl;

    // ========== Transfers (HA Sessions) ==========

    public async Task SaveSessionAsync(TransferSession session)
    {
        var key = SessionKey(session.TransferId);
        var json = JsonSerializer.Serialize(session);
        var ttl = session.ExpiresAtUtc - _time.GetUtcNow().UtcDateTime;

        if (ttl <= TimeSpan.Zero)
        {
            // Already expired - do not save, and ensure it is removed if it existed
            await _db.KeyDeleteAsync(key);
            return;
        }

        await _db.StringSetAsync(key, json, ttl);
        if (session.OpenedBySender)
        {
            await _db.SetAddAsync(OpenedByKey(session.SenderId), session.TransferId);
        }
    }

    private string SessionKey(string transferId) => $"{_keyPrefix}sessions:{transferId}";

    private string ClientKey(Guid clientId) => ClientKey(clientId.ToString());

    private string ClientKey(string clientId) => $"{_keyPrefix}clients:{clientId}";

    private string ConnKey(string connectionId) => $"{_keyPrefix}conn:{connectionId}";

    private string ClientIdsKey() => $"{_keyPrefix}client_ids";

    // The ids of the sessions a sender opened itself. An entry outlives a session whose TTL runs out,
    // so the set is only a way to the sessions, never a count: the sessions decide what counts, and the
    // count drops the entries they no longer back.
    private string OpenedByKey(Guid senderId) => $"{_keyPrefix}opened_by:{senderId}";

    public async Task<TransferSession?> GetSessionAsync(string transferId)
    {
        var key = SessionKey(transferId);
        var json = await _db.StringGetAsync(key);

        if (json.IsNullOrEmpty) return null;

        return JsonSerializer.Deserialize<TransferSession>(json.ToString()!);
    }

    public async Task<int> CountTransfersOpenedByAsync(Guid senderId)
    {
        var indexKey = OpenedByKey(senderId);
        var ids = await _db.SetMembersAsync(indexKey);
        if (ids.Length == 0) return 0;

        // Pipelined single-key GETs avoid Redis Cluster CROSSSLOT errors from multi-key MGET.
        var reads = new Task<RedisValue>[ids.Length];
        for (int i = 0; i < ids.Length; i++)
        {
            reads[i] = _db.StringGetAsync(SessionKey(ids[i].ToString()));
        }

        var sessions = await Task.WhenAll(reads);
        var gone = ids.Where((_, i) => !sessions[i].HasValue).ToArray();
        if (gone.Length > 0)
        {
            await _db.SetRemoveAsync(indexKey, gone);
        }

        return ids.Length - gone.Length;
    }

    public async Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry)
    {
        var key = SessionKey(transferId);
        var ttl = newExpiry - _time.GetUtcNow().UtcDateTime;

        if (ttl <= TimeSpan.Zero)
        {
            // If expired, ensure it's deleted
            await _db.KeyDeleteAsync(key);
            return;
        }

        // 1. Update TTL
        await _db.KeyExpireAsync(key, ttl);

        // 2. The payload's ExpiresAtUtc is left as written: rewriting it would fetch, deserialize
        // and save the session on every heartbeat. Nothing reads it back from this store; the
        // key's TTL is what decides whether the session is alive.
    }

    public async Task DeleteSessionAsync(string transferId)
    {
        var json = await _db.StringGetDeleteAsync(SessionKey(transferId));
        if (json.IsNullOrEmpty) return;

        // The count would drop the entry too; this keeps an ended transfer out of the index of a sender
        // that never opens another.
        var session = JsonSerializer.Deserialize<TransferSession>(json.ToString()!);
        if (session is { OpenedBySender: true })
        {
            await _db.SetRemoveAsync(OpenedByKey(session.SenderId), transferId);
        }
    }

    // --- Clients ---

    public async Task SaveClientAsync(ClientRegistration client)
    {
        var key = ClientKey(client.ClientId);
        var connKey = ConnKey(client.ConnectionId);
        var json = JsonSerializer.Serialize(client);
        var ttl = TtlFor(client);

        await _db.StringSetAsync(key, json, ttl);

        // Secondary index: connectionId -> clientId (for O(1) lookup)
        await _db.StringSetAsync(connKey, client.ClientId.ToString(), ttl);

        // Also add to a set for GetAllClients enumeration
        await _db.SetAddAsync(ClientIdsKey(), client.ClientId.ToString());
    }

    public async Task<ClientRegistration?> GetClientAsync(Guid clientId)
    {
        var key = ClientKey(clientId);
        var json = await _db.StringGetAsync(key);

        if (json.IsNullOrEmpty) return null;

        return JsonSerializer.Deserialize<ClientRegistration>(json.ToString()!);
    }

    public async Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc)
    {
        var connKey = ConnKey(connectionId);
        var clientIdStr = await _db.StringGetAsync(connKey);

        if (clientIdStr.IsNullOrEmpty) return;
        if (!Guid.TryParse(clientIdStr.ToString(), out var clientId)) return;

        var key = ClientKey(clientId);

        for (var attempt = 0; attempt < MaxWriteAttempts; attempt++)
        {
            var json = await _db.StringGetAsync(key);
            if (json.IsNullOrEmpty) return;

            var client = JsonSerializer.Deserialize<ClientRegistration>(json.ToString()!);
            if (client == null || client.ConnectionId != connectionId) return;

            var updated = client with { DisconnectedAtUtc = whenUtc };

            // Conditional write on the exact payload read above. A Connect that landed in between
            // stored a different one, the condition fails, and the next read finds the live
            // registration on another connection and stops; a renewal changed it under the same
            // connection, and the mark is still owed.
            var tx = _db.CreateTransaction();
            tx.AddCondition(Condition.StringEqual(key, json));
            _ = tx.StringSetAsync(key, JsonSerializer.Serialize(updated), TtlFor(updated));
            if (await tx.ExecuteAsync()) return;
        }

        throw new InvalidOperationException($"Registration {clientId} kept changing while its disconnection was recorded.");
    }

    public async Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
    {
        // 1. Lookup clientId from connectionId index
        var connKey = ConnKey(connectionId);
        var clientIdStr = await _db.StringGetAsync(connKey);

        if (clientIdStr.IsNullOrEmpty) return null;
        if (!Guid.TryParse(clientIdStr.ToString(), out var clientId)) return null;

        // 2. Get client by ID
        var client = await GetClientAsync(clientId);
        if (client == null) return null;

        // 3. The hub resolves a client by connection on every call it makes, so reaching here is
        //    proof the client is still talking to us. Push the expiry back: ConnectedTtlSeconds is
        //    a backstop for registrations orphaned by a hub that died, not an idle timer.
        //    A dropped registration is deliberately left to run out its grace period.
        if (!client.DisconnectedAtUtc.HasValue)
        {
            await _db.KeyExpireAsync(ClientKey(clientId), _connectedTtl);
            await _db.KeyExpireAsync(connKey, _connectedTtl);
        }

        return client;
    }

    public async Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync()
    {
        // 1. Get all IDs
        var clientIdsKey = ClientIdsKey();
        var ids = await _db.SetMembersAsync(clientIdsKey);
        if (ids.Length == 0) return Array.Empty<ClientRegistration>();

        // 2. Pipelined single-key GETs avoid Redis Cluster CROSSSLOT errors from multi-key MGET.
        var reads = new Task<RedisValue>[ids.Length];
        for (int i = 0; i < ids.Length; i++)
        {
            reads[i] = _db.StringGetAsync(ClientKey(ids[i].ToString()));
        }

        var values = await Task.WhenAll(reads);

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
            await _db.SetRemoveAsync(clientIdsKey, missingIds.ToArray());
        }

        return clients;
    }

    public async Task DeleteClientAsync(Guid clientId)
    {
        // Get client first to cleanup connection index
        var client = await GetClientAsync(clientId);

        var key = ClientKey(clientId);
        await _db.KeyDeleteAsync(key);
        await _db.SetRemoveAsync(ClientIdsKey(), clientId.ToString());

        // Cleanup connection index
        if (client != null)
        {
            await _db.KeyDeleteAsync(ConnKey(client.ConnectionId));
        }
    }

    public async Task<ClientRegistration?> RenewClientAsync(
        Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry)
    {
        var key = ClientKey(clientId);

        for (var attempt = 0; attempt < MaxWriteAttempts; attempt++)
        {
            var json = await _db.StringGetAsync(key);
            if (json.IsNullOrEmpty) return null;

            var client = JsonSerializer.Deserialize<ClientRegistration>(json.ToString()!);
            if (client == null || !string.Equals(client.Subject, expectedSubject, StringComparison.Ordinal)) return null;

            var renewed = client with { Groups = groups.ToHashSet(), TokenExpiry = tokenExpiry };

            // Conditional on the payload read above, and the connection index is left alone: only
            // the groups and the expiry change, and what a Connect or a disconnect wrote since
            // makes the condition fail and the read start again. The TTL stays what it was, since
            // renewing says nothing of whether the connection is up.
            var tx = _db.CreateTransaction();
            tx.AddCondition(Condition.StringEqual(key, json));
            _ = tx.StringSetAsync(key, JsonSerializer.Serialize(renewed), expiry: null, keepTtl: true);
            if (await tx.ExecuteAsync()) return renewed;
        }

        throw new InvalidOperationException($"Registration {clientId} kept changing while its token was renewed.");
    }
}

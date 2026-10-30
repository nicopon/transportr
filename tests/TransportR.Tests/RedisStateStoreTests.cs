using Microsoft.Extensions.Options;
using TransportR.Configuration;
using StackExchange.Redis;
using TransportR.Models;
using TransportR.Storage.Redis;

namespace TransportR.Tests;

/// <summary>
/// Integration tests for RedisStateStore, on a container or a local redis-server (see <see cref="RedisServer"/>).
/// Tests are automatically skipped if neither is available.
/// </summary>
[Trait("Category", "Integration")]
public class RedisStateStoreTests : IAsyncLifetime
{
    private RedisServer _redis = null!;
    private RedisStateStore _store = null!;
    private IConnectionMultiplexer _multiplexer = null!;

    public async Task InitializeAsync()
    {
        _redis = await RedisServer.StartAsync();
        try
        {
            _multiplexer = await ConnectWhenPublishedAsync(_redis.ConnectionString);
        }
        catch
        {
            // xUnit does not dispose a test whose initialization failed, and the server would outlive the run.
            await _redis.DisposeAsync();
            throw;
        }

        _store = new RedisStateStore(_multiplexer);
    }

    // A container answers from the inside before the port it publishes on the host does, and a
    // connect in that gap is refused at once rather than timing out.
    private static async Task<IConnectionMultiplexer> ConnectWhenPublishedAsync(string connectionString)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (true)
        {
            try
            {
                return await ConnectionMultiplexer.ConnectAsync(connectionString);
            }
            catch (RedisConnectionException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(50);
            }
        }
    }

    public async Task DisposeAsync()
    {
        _multiplexer?.Dispose();
        if (_redis != null) await _redis.DisposeAsync();
    }

    // ========== Client Tests ==========

    [RedisAvailableFact]
    public async Task SaveClient_ThenGetClient_ReturnsClient()
    {
        var clientId = Guid.NewGuid();
        var client = CreateClient(clientId, "conn-1");

        await _store.SaveClientAsync(client);
        var result = await _store.GetClientAsync(clientId);

        Assert.NotNull(result);
        Assert.Equal(clientId, result.ClientId);
        Assert.Equal("conn-1", result.ConnectionId);
    }

    [RedisAvailableFact]
    public async Task GetClientByConnectionId_ReturnsClient()
    {
        var clientId = Guid.NewGuid();
        var connectionId = $"conn-{Guid.NewGuid()}";
        var client = CreateClient(clientId, connectionId);
        await _store.SaveClientAsync(client);

        var result = await _store.GetClientByConnectionIdAsync(connectionId);

        Assert.NotNull(result);
        Assert.Equal(clientId, result.ClientId);
    }

    [RedisAvailableFact]
    public async Task GetClientByConnectionId_NotFound_ReturnsNull()
    {
        var result = await _store.GetClientByConnectionIdAsync($"non-existent-{Guid.NewGuid()}");
        Assert.Null(result);
    }

    [RedisAvailableFact]
    public async Task GetAllClients_ReturnsAllClients()
    {
        var client1 = CreateClient(Guid.NewGuid(), $"conn-{Guid.NewGuid()}");
        var client2 = CreateClient(Guid.NewGuid(), $"conn-{Guid.NewGuid()}");
        await _store.SaveClientAsync(client1);
        await _store.SaveClientAsync(client2);

        var all = await _store.GetAllClientsAsync();

        Assert.True(all.Count >= 2); // May have clients from other tests
    }

    [RedisAvailableFact]
    public async Task DeleteClient_RemovesFromBothIndexes()
    {
        var clientId = Guid.NewGuid();
        var connectionId = $"conn-del-{Guid.NewGuid()}";
        var client = CreateClient(clientId, connectionId);
        await _store.SaveClientAsync(client);

        await _store.DeleteClientAsync(clientId);

        Assert.Null(await _store.GetClientAsync(clientId));
        Assert.Null(await _store.GetClientByConnectionIdAsync(connectionId));
    }

    [RedisAvailableFact]
    public async Task RenewClient_ReplacesTheGroupsAndTheExpiry_AndNothingElse()
    {
        var clientId = Guid.NewGuid();
        var connectionId = $"conn-renew-{Guid.NewGuid()}";
        await _store.SaveClientAsync(CreateClient(clientId, connectionId) with { Subject = "subject" });
        await _store.MarkDisconnectedAsync(connectionId, DateTime.UtcNow);
        var ttlBefore = await _multiplexer.GetDatabase().KeyTimeToLiveAsync($"transportr:clients:{clientId}");
        var newExpiry = DateTime.UtcNow.AddHours(2);

        var renewed = await _store.RenewClientAsync(clientId, "subject", new HashSet<string> { "other-group" }, newExpiry);

        Assert.NotNull(renewed);
        var stored = await _store.GetClientAsync(clientId);
        Assert.NotNull(stored);
        Assert.True(Math.Abs((stored.TokenExpiry!.Value - newExpiry).TotalSeconds) < 1);
        Assert.Equal(new[] { "other-group" }, stored.Groups);
        Assert.Equal(connectionId, stored.ConnectionId);
        Assert.NotNull(stored.DisconnectedAtUtc);
        Assert.Equal(clientId, (await _store.GetClientByConnectionIdAsync(connectionId))?.ClientId);

        // The grace of a dropped registration does not start over because its token was renewed.
        var ttlAfter = await _multiplexer.GetDatabase().KeyTimeToLiveAsync($"transportr:clients:{clientId}");
        Assert.True(ttlAfter <= ttlBefore);
    }

    [RedisAvailableFact]
    public async Task RenewClient_UnderAnotherSubject_ChangesNothing()
    {
        var clientId = Guid.NewGuid();
        await _store.SaveClientAsync(CreateClient(clientId, $"conn-owner-{Guid.NewGuid()}") with { Subject = "owner" });

        Assert.Null(await _store.RenewClientAsync(clientId, "intruder", new HashSet<string> { "g" }, DateTime.UtcNow.AddDays(1)));
        Assert.Null(await _store.RenewClientAsync(clientId, null, new HashSet<string> { "g" }, DateTime.UtcNow.AddDays(1)));

        var stored = await _store.GetClientAsync(clientId);
        Assert.Equal(new[] { "test-group" }, stored!.Groups);
    }

    [RedisAvailableFact]
    public async Task RenewClient_OfAnUnknownClient_ReturnsNull()
    {
        Assert.Null(await _store.RenewClientAsync(Guid.NewGuid(), "subject", new HashSet<string> { "g" }, null));
    }

    [RedisAvailableFact]
    public async Task MarkDisconnected_RacingARenewal_IsNotLost()
    {
        const int rounds = 200;
        var lost = 0;

        for (var round = 0; round < rounds; round++)
        {
            var clientId = Guid.NewGuid();
            var connectionId = $"conn-race-{Guid.NewGuid()}";
            await _store.SaveClientAsync(CreateClient(clientId, connectionId) with { Subject = "subject" });

            await Task.WhenAll(
                Task.Run(() => _store.RenewClientAsync(clientId, "subject", new HashSet<string> { "g" }, DateTime.UtcNow.AddHours(2))),
                Task.Run(() => _store.MarkDisconnectedAsync(connectionId, DateTime.UtcNow)));

            var stored = (await _store.GetClientAsync(clientId))!;
            if (stored.DisconnectedAtUtc == null) lost++;
            Assert.Equal(new[] { "g" }, stored.Groups);
        }

        Assert.Equal(0, lost);
    }

    [RedisAvailableFact]
    public async Task MarkDisconnected_MarksTheRegistrationOfThatConnection()
    {
        var clientId = Guid.NewGuid();
        var connectionId = $"conn-mark-{Guid.NewGuid()}";
        await _store.SaveClientAsync(CreateClient(clientId, connectionId));

        await _store.MarkDisconnectedAsync(connectionId, DateTime.UtcNow);

        var stored = await _store.GetClientAsync(clientId);
        Assert.NotNull(stored);
        Assert.NotNull(stored.DisconnectedAtUtc);
    }

    [RedisAvailableFact]
    public async Task MarkDisconnected_AfterTheClientReconnected_LeavesTheLiveRegistrationAlone()
    {
        // Same race as in memory, and the one this store used to lose outright: it keeps the
        // stale conn: index, so a resurrected ConnectionId still answers Heartbeat with true and
        // the client never learns it has to register again.
        var clientId = Guid.NewGuid();
        var oldConnection = $"conn-old-{Guid.NewGuid()}";
        var newConnection = $"conn-new-{Guid.NewGuid()}";
        await _store.SaveClientAsync(CreateClient(clientId, oldConnection));
        await _store.SaveClientAsync(CreateClient(clientId, newConnection));

        await _store.MarkDisconnectedAsync(oldConnection, DateTime.UtcNow);

        var stored = await _store.GetClientAsync(clientId);
        Assert.NotNull(stored);
        Assert.Equal(newConnection, stored.ConnectionId);
        Assert.Null(stored.DisconnectedAtUtc);
    }

    // ========== Session Tests (HA) ==========

    [RedisAvailableFact]
    public async Task SaveSession_ThenGetSession_ReturnsSession()
    {
        var transferId = $"transfer-{Guid.NewGuid()}";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));

        await _store.SaveSessionAsync(session);
        var result = await _store.GetSessionAsync(transferId);

        Assert.NotNull(result);
        Assert.Equal(session.SenderId, result.SenderId);
    }

    [RedisAvailableFact]
    public async Task GetSession_Expired_ReturnsNull()
    {
        // Expired 5 minutes ago
        var transferId = $"transfer-expired-{Guid.NewGuid()}";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow.AddMinutes(-10), DateTime.UtcNow.AddMinutes(-5));

        await _store.SaveSessionAsync(session);
        
        // Redis should have expired the key immediately or upon access
        var result = await _store.GetSessionAsync(transferId);

        Assert.Null(result);
    }
    
    [RedisAvailableFact]
    public async Task ExtendSessionExpiry_UpdatesTTL()
    {
        var transferId = $"transfer-extend-{Guid.NewGuid()}";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddSeconds(2)); // Short TTL
        await _store.SaveSessionAsync(session);
        
        var newExpiry = DateTime.UtcNow.AddHours(1);

        await _store.ExtendSessionExpiryAsync(transferId, newExpiry);

        var updated = await _store.GetSessionAsync(transferId);
        Assert.NotNull(updated);
        
        // Note: RedisStateStore implementation does NOT update the JSON payload for 'ExpiresAtUtc', only the Redis TTL.
        // So the returned object might still have old ExpiresAtUtc if we check property.
        // But the KEY exists.
        // The implementation detail: GetSessionAsync returns deserialized JSON. If JSON wasn't updated, ExpiresAtUtc is old.
        // But ExtendSessionExpiryAsync in RedisStateStore does `EXPIREAT`. It does NOT update the JSON.
        // So checking `updated.ExpiresAtUtc` would fail if we expect it to change.
        // We can check if the key is still there after the original expiry time passed.
        
        await Task.Delay(3000); // Wait longer than original TTL (2s)
        
        var resultAfterOriginalTTL = await _store.GetSessionAsync(transferId);
        Assert.NotNull(resultAfterOriginalTTL); // Should still be alive due to extension
    }

    [RedisAvailableFact]
    public async Task DeleteSession_RemovesSession()
    {
        var transferId = $"transfer-del-{Guid.NewGuid()}";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));
        await _store.SaveSessionAsync(session);

        await _store.DeleteSessionAsync(transferId);

        Assert.Null(await _store.GetSessionAsync(transferId));
    }

    // ========== Helpers ==========

    private static ClientRegistration CreateClient(Guid clientId, string connectionId)
    {
        return new ClientRegistration(
            clientId,
            connectionId,
            new HashSet<string> { "test-group" },
            null,
            DateTime.UtcNow.AddHours(1),
            DateTime.UtcNow);
    }

    // ---- Registration lifetime ----

    private RedisStateStore StoreWith(int connectedTtlSeconds, int graceSeconds) =>
        new(_multiplexer, Options.Create(new ClientRegistryConfiguration
        {
            ConnectedTtlSeconds = connectedTtlSeconds,
            DisconnectedGraceSeconds = graceSeconds
        }));

    [RedisAvailableFact]
    public async Task SaveClient_UsesTheConnectedTtl_WhenStillConnected()
    {
        // Arrange
        var store = StoreWith(connectedTtlSeconds: 3600, graceSeconds: 60);
        var clientId = Guid.NewGuid();

        // Act
        await store.SaveClientAsync(
            new ClientRegistration(clientId, "conn-1", ["senders"], null, null, DateTime.UtcNow));

        // Assert: a connected client must not be evicted on a short timer - idling between
        // transfers is not the same as being gone.
        var ttl = await _multiplexer.GetDatabase().KeyTimeToLiveAsync($"transportr:clients:{clientId}");
        Assert.NotNull(ttl);
        Assert.True(ttl > TimeSpan.FromMinutes(30), $"Expected the connected TTL, got {ttl}");
    }

    [RedisAvailableFact]
    public async Task SaveClient_UsesTheGrace_OnceDisconnected()
    {
        // Arrange
        var store = StoreWith(connectedTtlSeconds: 3600, graceSeconds: 60);
        var clientId = Guid.NewGuid();

        // Act: what OnDisconnectedAsync writes.
        await store.SaveClientAsync(
            new ClientRegistration(clientId, "conn-1", ["senders"], null, null, DateTime.UtcNow, DateTime.UtcNow));

        // Assert: kept long enough for a reconnect to claim it back, then evicted by Redis.
        var ttl = await _multiplexer.GetDatabase().KeyTimeToLiveAsync($"transportr:clients:{clientId}");
        Assert.NotNull(ttl);
        Assert.True(ttl <= TimeSpan.FromSeconds(60), $"Expected the grace TTL, got {ttl}");
    }

    [RedisAvailableFact]
    public async Task GetClientByConnectionId_PushesTheExpiryBack_ForAConnectedClient()
    {
        // Arrange: a registration already close to expiring.
        var store = StoreWith(connectedTtlSeconds: 3600, graceSeconds: 60);
        var clientId = Guid.NewGuid();
        await store.SaveClientAsync(
            new ClientRegistration(clientId, "conn-1", ["senders"], null, null, DateTime.UtcNow));

        var db = _multiplexer.GetDatabase();
        await db.KeyExpireAsync($"transportr:clients:{clientId}", TimeSpan.FromSeconds(30));
        await db.KeyExpireAsync("transportr:conn:conn-1", TimeSpan.FromSeconds(30));

        // Act: the hub resolves the client by connection on every call it makes.
        var found = await store.GetClientByConnectionIdAsync("conn-1");

        // Assert
        Assert.NotNull(found);
        var ttl = await db.KeyTimeToLiveAsync($"transportr:clients:{clientId}");
        Assert.True(ttl > TimeSpan.FromMinutes(30), $"Expected the expiry to be pushed back, got {ttl}");
    }
}

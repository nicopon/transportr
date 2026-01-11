using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Testcontainers.Redis;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Storage.Redis;

namespace TransportR.Tests;

/// <summary>
/// Integration tests for RedisStateStore using Testcontainers.
/// Tests are automatically skipped if Docker is not available.
/// </summary>
[Trait("Category", "Integration")]
public class RedisStateStoreTests : IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder()
        .WithImage("redis:7.0")
        .Build();

    private RedisStateStore _store = null!;
    private IConnectionMultiplexer _multiplexer = null!;
    private readonly ILogger<RedisStateStore> _logger = new LoggerFactory().CreateLogger<RedisStateStore>();

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());
        _store = new RedisStateStore(_multiplexer, _logger);
    }

    public async Task DisposeAsync()
    {
        _multiplexer?.Dispose();
        await _redis.DisposeAsync();
    }

    // ========== Client Tests ==========

    [DockerAvailableFact]
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

    [DockerAvailableFact]
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

    [DockerAvailableFact]
    public async Task GetClientByConnectionId_NotFound_ReturnsNull()
    {
        var result = await _store.GetClientByConnectionIdAsync($"non-existent-{Guid.NewGuid()}");
        Assert.Null(result);
    }

    [DockerAvailableFact]
    public async Task GetAllClients_ReturnsAllClients()
    {
        var client1 = CreateClient(Guid.NewGuid(), $"conn-{Guid.NewGuid()}");
        var client2 = CreateClient(Guid.NewGuid(), $"conn-{Guid.NewGuid()}");
        await _store.SaveClientAsync(client1);
        await _store.SaveClientAsync(client2);

        var all = await _store.GetAllClientsAsync();

        Assert.True(all.Count >= 2); // May have clients from other tests
    }

    [DockerAvailableFact]
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

    [DockerAvailableFact]
    public async Task UpdateClientTokenExpiry_UpdatesExpiry()
    {
        var clientId = Guid.NewGuid();
        var client = CreateClient(clientId, $"conn-exp-{Guid.NewGuid()}");
        await _store.SaveClientAsync(client);
        var newExpiry = DateTime.UtcNow.AddHours(2);

        await _store.UpdateClientTokenExpiryAsync(clientId, newExpiry);

        var updated = await _store.GetClientAsync(clientId);
        Assert.NotNull(updated);
        // Allow 1 second tolerance for serialization round-trip
        Assert.True(Math.Abs((updated.TokenExpiry!.Value - newExpiry).TotalSeconds) < 1);
    }

    // ========== Session Tests (HA) ==========

    [DockerAvailableFact]
    public async Task SaveSession_ThenGetSession_ReturnsSession()
    {
        var transferId = $"transfer-{Guid.NewGuid()}";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));

        await _store.SaveSessionAsync(session);
        var result = await _store.GetSessionAsync(transferId);

        Assert.NotNull(result);
        Assert.Equal(session.SenderId, result.SenderId);
    }

    [DockerAvailableFact]
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
    
    [DockerAvailableFact]
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

    [DockerAvailableFact]
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
}

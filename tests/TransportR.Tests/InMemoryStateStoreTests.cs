using Microsoft.Extensions.Options;
using TransportR.Configuration;
using TransportR.Services;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Tests;

/// <summary>
/// Unit tests for InMemoryStateStore (HA state management).
/// </summary>
public class InMemoryStateStoreTests
{
    private readonly InMemoryStateStore _store = new();

    // ========== Client Tests ==========

    [Fact]
    public async Task SaveClient_ThenGetClient_ReturnsClient()
    {
        // Arrange
        var clientId = Guid.NewGuid();
        var client = CreateClient(clientId, "conn-1");

        // Act
        await _store.SaveClientAsync(client);
        var result = await _store.GetClientAsync(clientId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(clientId, result.ClientId);
        Assert.Equal("conn-1", result.ConnectionId);
    }

    [Fact]
    public async Task GetClientByConnectionId_ReturnsClient()
    {
        // Arrange
        var clientId = Guid.NewGuid();
        var connectionId = "conn-lookup";
        var client = CreateClient(clientId, connectionId);
        await _store.SaveClientAsync(client);

        // Act
        var result = await _store.GetClientByConnectionIdAsync(connectionId);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(clientId, result.ClientId);
    }

    [Fact]
    public async Task GetClientByConnectionId_NotFound_ReturnsNull()
    {
        var result = await _store.GetClientByConnectionIdAsync("non-existent");
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAllClients_ReturnsAllClients()
    {
        // Arrange
        var client1 = CreateClient(Guid.NewGuid(), "conn-1");
        var client2 = CreateClient(Guid.NewGuid(), "conn-2");
        await _store.SaveClientAsync(client1);
        await _store.SaveClientAsync(client2);

        // Act
        var all = await _store.GetAllClientsAsync();

        // Assert
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task DeleteClient_RemovesFromBothIndexes()
    {
        // Arrange
        var clientId = Guid.NewGuid();
        var connectionId = "conn-del";
        var client = CreateClient(clientId, connectionId);
        await _store.SaveClientAsync(client);

        // Act
        await _store.DeleteClientAsync(clientId);

        // Assert
        Assert.Null(await _store.GetClientAsync(clientId));
        Assert.Null(await _store.GetClientByConnectionIdAsync(connectionId));
    }

    [Fact]
    public async Task UpdateClientTokenExpiry_UpdatesExpiry()
    {
        // Arrange
        var clientId = Guid.NewGuid();
        var client = CreateClient(clientId, "conn-exp");
        await _store.SaveClientAsync(client);
        var newExpiry = DateTime.UtcNow.AddHours(2);

        // Act
        await _store.UpdateClientTokenExpiryAsync(clientId, newExpiry);

        // Assert
        var updated = await _store.GetClientAsync(clientId);
        Assert.NotNull(updated);
        Assert.Equal(newExpiry, updated.TokenExpiry);
    }

    // ========== Session Tests (HA) ==========

    [Fact]
    public async Task SaveSession_ThenGetSession_ReturnsSession()
    {
        // Arrange
        var session = new TransferSession("transfer-1", Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));

        // Act
        await _store.SaveSessionAsync(session);
        var result = await _store.GetSessionAsync("transfer-1");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(session.SenderId, result.SenderId);
    }
    
    [Fact]
    public async Task GetSession_Expired_ReturnsNull()
    {
        // Arrange - Create expired session
        var session = new TransferSession("transfer-expired", Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow.AddMinutes(-5), DateTime.UtcNow.AddMinutes(-1));

        // Act
        await _store.SaveSessionAsync(session);
        var result = await _store.GetSessionAsync("transfer-expired");

        // Assert
        Assert.Null(result); // InMemoryStore implements Lazy Expiry
    }
    
    [Fact]
    public async Task ExtendSessionExpiry_UpdatesExpiry()
    {
        // Arrange
        var transferId = "transfer-extend";
        var session = new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(1));
        await _store.SaveSessionAsync(session);
        
        var newExpiry = DateTime.UtcNow.AddHours(1);

        // Act
        await _store.ExtendSessionExpiryAsync(transferId, newExpiry);

        // Assert
        var updated = await _store.GetSessionAsync(transferId);
        Assert.NotNull(updated);
        Assert.Equal(newExpiry, updated.ExpiresAtUtc);
    }

    [Fact]
    public async Task DeleteSession_RemovesSession()
    {
        // Arrange
        var session = new TransferSession("transfer-del", Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5));
        await _store.SaveSessionAsync(session);

        // Act
        await _store.DeleteSessionAsync("transfer-del");

        // Assert
        Assert.Null(await _store.GetSessionAsync("transfer-del"));
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

    private static InMemoryStateStore StoreWithGrace(int graceSeconds) =>
        new(Options.Create(new ClientRegistryConfiguration { DisconnectedGraceSeconds = graceSeconds }));

    private static ClientRegistration Registration(Guid clientId, string connectionId, DateTime? disconnectedAt = null) =>
        new(clientId, connectionId, ["senders"], null, null, DateTime.UtcNow, disconnectedAt);

    [Fact]
    public async Task SaveClient_OnReconnect_DropsTheStaleConnectionIndexEntry()
    {
        // Arrange: same client, new connection - what a reconnect looks like.
        var clientId = Guid.NewGuid();
        await _store.SaveClientAsync(Registration(clientId, "conn-old"));

        // Act
        await _store.SaveClientAsync(Registration(clientId, "conn-new"));

        // Assert: the old connection id no longer resolves, the new one does.
        Assert.Null(await _store.GetClientByConnectionIdAsync("conn-old"));
        var current = await _store.GetClientByConnectionIdAsync("conn-new");
        Assert.Equal(clientId, current!.ClientId);
    }

    [Fact]
    public async Task GetClient_WhileWithinGrace_StillReturnsADisconnectedClient()
    {
        // Arrange: dropped a moment ago. A reconnecting client must find its registration.
        var store = StoreWithGrace(300);
        var clientId = Guid.NewGuid();
        await store.SaveClientAsync(Registration(clientId, "conn-1", DateTime.UtcNow));

        // Assert
        Assert.NotNull(await store.GetClientAsync(clientId));
        Assert.Single(await store.GetAllClientsAsync());
    }

    [Fact]
    public async Task GetClient_PastGrace_EvictsTheRegistration()
    {
        // Arrange: dropped longer ago than the grace period allows.
        var store = StoreWithGrace(60);
        var clientId = Guid.NewGuid();
        await store.SaveClientAsync(
            Registration(clientId, "conn-1", DateTime.UtcNow.AddSeconds(-120)));

        // Assert: gone from every lookup, not just filtered out of one.
        Assert.Null(await store.GetClientAsync(clientId));
        Assert.Null(await store.GetClientByConnectionIdAsync("conn-1"));
        Assert.Empty(await store.GetAllClientsAsync());
    }

    [Fact]
    public async Task GetAllClients_NeverEvictsAConnectedClient()
    {
        // Arrange: connected, registered long ago, idle ever since. Nothing should remove it -
        // a caller that finds nobody fails outright, whereas one trying a dead client retries.
        var store = StoreWithGrace(1);
        var clientId = Guid.NewGuid();
        await store.SaveClientAsync(
            new ClientRegistration(clientId, "conn-1", ["senders"], null, null, DateTime.UtcNow.AddHours(-9)));

        // Assert
        Assert.Single(await store.GetAllClientsAsync());
        Assert.NotNull(await store.GetClientAsync(clientId));
    }
}

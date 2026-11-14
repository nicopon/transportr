using Microsoft.Extensions.Options;
using TransportR.Hub;

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
    public async Task RenewClient_ReplacesTheGroupsAndTheExpiry_AndNothingElse()
    {
        var clientId = Guid.NewGuid();
        await _store.SaveClientAsync(CreateClient(clientId, "conn-renew") with { Subject = "subject" });
        var disconnectedAt = DateTime.UtcNow;
        await _store.MarkDisconnectedAsync("conn-renew", disconnectedAt);
        var newExpiry = DateTime.UtcNow.AddHours(2);

        var renewed = await _store.RenewClientAsync(clientId, "subject", new HashSet<string> { "other-group" }, newExpiry);

        Assert.NotNull(renewed);
        var stored = await _store.GetClientAsync(clientId);
        Assert.NotNull(stored);
        Assert.Equal(renewed, stored);
        Assert.Equal(newExpiry, stored.TokenExpiry);
        Assert.Equal(new[] { "other-group" }, stored.Groups);
        Assert.Equal("conn-renew", stored.ConnectionId);
        Assert.Equal(disconnectedAt, stored.DisconnectedAtUtc);
        Assert.Equal(clientId, (await _store.GetClientByConnectionIdAsync("conn-renew"))?.ClientId);
    }

    [Fact]
    public async Task RenewClient_UnderAnotherSubject_ChangesNothing()
    {
        var clientId = Guid.NewGuid();
        var registered = CreateClient(clientId, "conn-owner") with { Subject = "owner" };
        await _store.SaveClientAsync(registered);

        Assert.Null(await _store.RenewClientAsync(clientId, "intruder", new HashSet<string> { "g" }, DateTime.UtcNow.AddDays(1)));
        Assert.Null(await _store.RenewClientAsync(clientId, null, new HashSet<string> { "g" }, DateTime.UtcNow.AddDays(1)));

        Assert.Equal(registered, await _store.GetClientAsync(clientId));
    }

    [Fact]
    public async Task RenewClient_TakesTwoAbsentSubjectsForTheSame()
    {
        var clientId = Guid.NewGuid();
        await _store.SaveClientAsync(CreateClient(clientId, "conn-dev"));

        Assert.NotNull(await _store.RenewClientAsync(clientId, null, new HashSet<string> { "g" }, null));
        Assert.Null(await _store.RenewClientAsync(clientId, "someone", new HashSet<string> { "g" }, null));
    }

    [Fact]
    public async Task RenewClient_OfAnUnknownClient_ReturnsNull()
    {
        Assert.Null(await _store.RenewClientAsync(Guid.NewGuid(), "subject", new HashSet<string> { "g" }, null));
    }

    [Fact]
    public async Task MarkDisconnected_RacingARenewal_IsNotLost()
    {
        // The renewal changes the payload the mark was computed from, under the very connection the
        // mark is for. A mark that gave up on that would leave a dead connection looking live, and
        // nothing would ever evict it. The window is a few instructions wide, so the race is run
        // many times.
        const int rounds = 2000;
        var lost = 0;

        for (var round = 0; round < rounds; round++)
        {
            var clientId = Guid.NewGuid();
            var connectionId = $"conn-race-{round}";
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
    public async Task ExtendSessionExpiry_RacingADelete_DoesNotBringTheSessionBack()
    {
        // The extension reads the clock between its read of the session and its write: the clock
        // deletes the session there, which puts the delete in a window a few instructions wide on
        // every run. A session brought back stays for the whole TTL, and a stream that reopens
        // finds the ended transfer again.
        var clock = new HookedClock();
        var store = new InMemoryStateStore(timeProvider: clock);
        const string transferId = "transfer-extend-delete";
        await store.SaveSessionAsync(new TransferSession(transferId, Guid.NewGuid(), Guid.NewGuid(), DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5)));

        clock.OnNextRead = () => store.DeleteSessionAsync(transferId).GetAwaiter().GetResult();
        await store.ExtendSessionExpiryAsync(transferId, DateTime.UtcNow.AddMinutes(10));

        Assert.True(clock.Fired, "the delete did not run inside the extension");
        Assert.Null(await store.GetSessionAsync(transferId));
    }

    private sealed class HookedClock : TimeProvider
    {
        public Action? OnNextRead;
        public bool Fired;

        public override DateTimeOffset GetUtcNow()
        {
            if (Interlocked.Exchange(ref OnNextRead, null) is { } hook)
            {
                hook();
                Fired = true;
            }

            return base.GetUtcNow();
        }
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

    [Fact]
    public async Task CountTransfersOpenedBy_CountsOnlyTheLiveSessionsTheSenderOpenedItself()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 12, 0, 0, TimeSpan.Zero));
        var store = new InMemoryStateStore(timeProvider: clock);
        var sender = Guid.NewGuid();
        var now = clock.GetUtcNow().UtcDateTime;
        TransferSession Session(string id, Guid senderId, bool openedBySender, TimeSpan lifetime) =>
            new(id, senderId, Guid.NewGuid(), now, now + lifetime) { OpenedBySender = openedBySender };

        await store.SaveSessionAsync(Session("open-1", sender, true, TimeSpan.FromMinutes(5)));
        await store.SaveSessionAsync(Session("open-2", sender, true, TimeSpan.FromMinutes(5)));
        await store.SaveSessionAsync(Session("by-the-host", sender, false, TimeSpan.FromMinutes(5)));
        await store.SaveSessionAsync(Session("another-sender", Guid.NewGuid(), true, TimeSpan.FromMinutes(5)));
        await store.SaveSessionAsync(Session("deleted", sender, true, TimeSpan.FromMinutes(5)));
        await store.SaveSessionAsync(Session("expiring", sender, true, TimeSpan.FromMinutes(1)));
        await store.DeleteSessionAsync("deleted");

        Assert.Equal(3, await store.CountTransfersOpenedByAsync(sender));

        clock.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal(2, await store.CountTransfersOpenedByAsync(sender));
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
    public async Task MarkDisconnected_MarksTheRegistrationOfThatConnection()
    {
        var clientId = Guid.NewGuid();
        await _store.SaveClientAsync(Registration(clientId, "conn-1"));

        await _store.MarkDisconnectedAsync("conn-1", DateTime.UtcNow);

        var stored = await _store.GetClientAsync(clientId);
        Assert.NotNull(stored);
        Assert.NotNull(stored.DisconnectedAtUtc);
    }

    [Fact]
    public async Task MarkDisconnected_OnAnUnknownConnection_DoesNothing()
    {
        await _store.MarkDisconnectedAsync("never-seen", DateTime.UtcNow);

        Assert.Empty(await _store.GetAllClientsAsync());
    }

    [Fact]
    public async Task MarkDisconnected_AfterTheClientReconnected_LeavesTheLiveRegistrationAlone()
    {
        // The disconnect callback for the old connection can run after the new Connect has
        // landed. Writing back the registration it read would reinstate the dead ConnectionId,
        // and the hub would then hold a connection nobody is listening on.
        var clientId = Guid.NewGuid();
        await _store.SaveClientAsync(Registration(clientId, "conn-old"));
        await _store.SaveClientAsync(Registration(clientId, "conn-new"));

        await _store.MarkDisconnectedAsync("conn-old", DateTime.UtcNow);

        var stored = await _store.GetClientAsync(clientId);
        Assert.NotNull(stored);
        Assert.Equal("conn-new", stored.ConnectionId);
        Assert.Null(stored.DisconnectedAtUtc);
        Assert.NotNull(await _store.GetClientByConnectionIdAsync("conn-new"));
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

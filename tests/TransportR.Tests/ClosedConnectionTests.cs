using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.Hub;
using TransportR.Hub.Services;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A stream the hub ends on purpose (a token that ran out) closes cleanly, and is no failure to
/// answer: nothing brings such a client back unless the client does.
/// </summary>
public class ClosedConnectionTests
{
    /// <summary>
    /// The state store, counting registrations, and closing the stream of the next ones the moment
    /// they are written: a hub that closes what it has just accepted.
    /// </summary>
    private sealed class ClosingStore : IStateStore
    {
        private readonly InMemoryStateStore _inner = new();
        private int _count;
        private int _closeNext;

        public ClientStreams? Streams { get; set; }

        public int Count => Volatile.Read(ref _count);

        public void CloseNext(int registrations) => Volatile.Write(ref _closeNext, registrations);

        public async Task SaveClientAsync(ClientRegistration client)
        {
            await _inner.SaveClientAsync(client);
            Interlocked.Increment(ref _count);
            if (Interlocked.Decrement(ref _closeNext) >= 0) Streams!.TryClose(client.ConnectionId);
        }

        public Task<ClientRegistration?> GetClientAsync(Guid clientId) => _inner.GetClientAsync(clientId);
        public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId) => _inner.GetClientByConnectionIdAsync(connectionId);
        public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync() => _inner.GetAllClientsAsync();
        public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc) => _inner.MarkDisconnectedAsync(connectionId, whenUtc);
        public Task DeleteClientAsync(Guid clientId) => _inner.DeleteClientAsync(clientId);
        public Task<ClientRegistration?> RenewClientAsync(Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry) =>
            _inner.RenewClientAsync(clientId, expectedSubject, groups, tokenExpiry);
        public Task SaveSessionAsync(TransferSession session) => _inner.SaveSessionAsync(session);
        public Task<TransferSession?> GetSessionAsync(string transferId) => _inner.GetSessionAsync(transferId);
        public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry) => _inner.ExtendSessionExpiryAsync(transferId, newExpiry);
        public Task DeleteSessionAsync(string transferId) => _inner.DeleteSessionAsync(transferId);
    }

    private static async Task<(LoopbackHost Host, ClosingStore Store)> StartHubAsync()
    {
        var store = new ClosingStore();
        var host = await AuthenticatedHubServerFactory.StartLoopbackAsync(
            configureServices: services => services.Replace(ServiceDescriptor.Singleton<IStateStore>(store)));
        store.Streams = host.Services.GetRequiredService<ClientStreams>();
        return (host, store);
    }

    private static async Task<T> EventuallyAsync<T>(Func<Task<T>> read, Func<T, bool> done, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var value = await read();
        while (!done(value) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
            value = await read();
        }

        return value;
    }

    [Fact]
    public async Task ConnectionClosedByTheHub_IsStartedAgain_WithTheClientRegisteredOnIt()
    {
        var (host, store) = await StartHubAsync();
        await using var _ = host;
        var tokens = new TokenSource("closed-principal", TimeSpan.FromHours(1));
        await using var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync);

        await client.ConnectAsync();
        var before = (await store.GetClientAsync(client.ClientId))!.ConnectionId;

        Assert.True(store.Streams!.TryClose(before));

        var after = await EventuallyAsync(
            async () => await store.GetClientAsync(client.ClientId),
            r => r != null && r.ConnectionId != before && r.DisconnectedAtUtc == null,
            TimeSpan.FromSeconds(15));

        Assert.NotEqual(before, after!.ConnectionId);
        Assert.Null(after.DisconnectedAtUtc);
        Assert.NotNull(await client.GetReceiversAsync());
    }

    [Fact]
    public async Task ConnectionClosedAgainWhileItIsBeingBroughtBack_IsStartedYetAgain()
    {
        // A hub that closes what it has just accepted (a token still not good) must not leave the client
        // out: the closure that lands as a stream comes back is not lost.
        var (host, store) = await StartHubAsync();
        await using var _ = host;
        var tokens = new TokenSource("closed-twice-principal", TimeSpan.FromHours(1));
        await using var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync);

        await client.ConnectAsync();
        var before = (await store.GetClientAsync(client.ClientId))!.ConnectionId;
        store.CloseNext(2);

        Assert.True(store.Streams!.TryClose(before));

        var after = await EventuallyAsync(
            async () => await store.GetClientAsync(client.ClientId),
            r => r != null && r.ConnectionId != before && r.DisconnectedAtUtc == null && store.Count >= 4,
            TimeSpan.FromSeconds(30));

        Assert.NotEqual(before, after!.ConnectionId);
        Assert.Null(after.DisconnectedAtUtc);
        Assert.True(store.Count >= 4, $"{store.Count} registrations.");
        Assert.True(client.Channel.IsOpen);
        Assert.NotNull(await client.GetReceiversAsync());
    }

    [Fact]
    public async Task ConnectionClosedWhileTheClientIsBeingDisposed_IsNotStartedAgain()
    {
        var (host, store) = await StartHubAsync();
        await using var _ = host;
        var tokens = new TokenSource("leaving-principal", TimeSpan.FromHours(1));
        var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync);
        await client.ConnectAsync();

        await client.DisposeAsync();
        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Equal(1, store.Count);
    }
}

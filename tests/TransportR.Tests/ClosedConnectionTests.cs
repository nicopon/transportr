using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A connection the hub ends on purpose closes cleanly, and SignalR's automatic reconnection only
/// answers failures: nothing brings such a client back unless the client does.
/// </summary>
public class ClosedConnectionTests
{
    private sealed class ConnectionCounter
    {
        private int _count;
        private int _closeNext;

        public int Count => Volatile.Read(ref _count);

        public void Add() => Interlocked.Increment(ref _count);

        /// <summary>The next <paramref name="connections"/> connections are closed the moment they open.</summary>
        public void CloseNext(int connections) => Volatile.Write(ref _closeNext, connections);

        public bool TakeClose() => Interlocked.Decrement(ref _closeNext) >= 0;
    }

    private class ClosingHub : CommandHub
    {
        private readonly ConnectionCounter _counter;

        public ClosingHub(
            IStateStore stateStore,
            ITransferManager transferManager,
            IClientIdentityProvider identityProvider,
            IBridgeManager bridgeManager,
            SignalRTransferInitiator initiator,
            AccessPolicy policy,
            ConnectionRegistry connections,
            IOptions<AffinityOptions> affinityOptions,
            ILogger<CommandHub> logger,
            TimeProvider timeProvider,
            ConnectionCounter counter)
            : base(stateStore, transferManager, identityProvider, bridgeManager, initiator, policy, connections, affinityOptions, logger, timeProvider)
        {
            _counter = counter;
        }

        public override Task OnConnectedAsync()
        {
            _counter.Add();
            if (_counter.TakeClose()) Context.Abort();
            return base.OnConnectedAsync();
        }

        // What the hub does to a connection whose token has run out.
        public Task CloseMe()
        {
            Context.Abort();
            return Task.CompletedTask;
        }
    }

    private static Task<LoopbackHost> StartHubAsync() => AuthenticatedHubServerFactory.StartLoopbackAsync(hub =>
    {
        hub.Services.AddSingleton<ConnectionCounter>();
        hub.UseHub<ClosingHub>();
    });

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

    [Theory]
    [InlineData(HttpTransportType.WebSockets)]
    [InlineData(HttpTransportType.ServerSentEvents)]
    [InlineData(HttpTransportType.LongPolling)]
    public async Task ConnectionClosedByTheHub_IsStartedAgain_WithTheClientRegisteredOnIt(HttpTransportType transport)
    {
        await using var host = await StartHubAsync();
        var store = host.Services.GetRequiredService<IStateStore>();
        var tokens = new TokenSource("closed-principal", TimeSpan.FromHours(1));
        await using var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync,
            configureConnection: options => options.Transports = transport);

        await client.ConnectAsync();
        var before = (await store.GetClientAsync(client.ClientId))!.ConnectionId;

        try
        {
            await client.ControlConnection.InvokeAsync(nameof(ClosingHub.CloseMe));
        }
        catch (Exception)
        {
            // The invocation ends with the connection it was made on.
        }

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
        // out: the closure that lands while a restart is running is not lost.
        await using var host = await StartHubAsync();
        var store = host.Services.GetRequiredService<IStateStore>();
        var counter = host.Services.GetRequiredService<ConnectionCounter>();
        var tokens = new TokenSource("closed-twice-principal", TimeSpan.FromHours(1));
        await using var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync,
            configureConnection: options => options.Transports = HttpTransportType.WebSockets);

        await client.ConnectAsync();
        var before = (await store.GetClientAsync(client.ClientId))!.ConnectionId;
        counter.CloseNext(2);

        try
        {
            await client.ControlConnection.InvokeAsync(nameof(ClosingHub.CloseMe));
        }
        catch (Exception)
        {
            // The invocation ends with the connection it was made on.
        }

        var after = await EventuallyAsync(
            async () => await store.GetClientAsync(client.ClientId),
            r => r != null && r.ConnectionId != before && r.DisconnectedAtUtc == null && counter.Count >= 4,
            TimeSpan.FromSeconds(30));

        Assert.NotEqual(before, after!.ConnectionId);
        Assert.Null(after.DisconnectedAtUtc);
        Assert.True(counter.Count >= 4, $"{counter.Count} connections.");
        Assert.NotNull(await client.GetReceiversAsync());
    }

    [Fact]
    public async Task ConnectionClosedWhileTheClientIsBeingDisposed_IsNotStartedAgain()
    {
        await using var host = await StartHubAsync();
        var counter = host.Services.GetRequiredService<ConnectionCounter>();
        var tokens = new TokenSource("leaving-principal", TimeSpan.FromHours(1));
        var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync);
        await client.ConnectAsync();

        await client.DisposeAsync();
        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Equal(1, counter.Count);
    }
}

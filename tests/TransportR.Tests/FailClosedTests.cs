using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Abstractions;
using TransportR.Client.Services;
using TransportR.Hub;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What the hub does when a client's right to act runs out or is taken away, end to end: real
/// clients with real short-lived tokens, a hub that sweeps every second, a socket that WebSockets
/// can use. Nothing here depends on how the hub is asked to act; it is the sweep or the renewal.
/// </summary>
public class FailClosedTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    private sealed class Rules(Func<bool> allowed) : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => allowed();
    }

    private sealed class Pair(
        LoopbackHost host, TransportRClient<string> sender, TransportRClient<string> receiver,
        Send<string> send, Receive<string> receive) : IAsyncDisposable
    {
        public LoopbackHost Host { get; } = host;
        public TransportRClient<string> Sender { get; } = sender;
        public TransportRClient<string> Receiver { get; } = receiver;
        public Send<string> Send { get; } = send;
        public Receive<string> Receive { get; } = receive;

        public async ValueTask DisposeAsync()
        {
            await Send.DisposeAsync();
            await Receive.DisposeAsync();
            await Sender.DisposeAsync();
            await Receiver.DisposeAsync();
        }
    }

    private static Task<LoopbackHost> StartHubAsync(int sweepSeconds = 1, Func<bool>? flowControl = null) =>
        AuthenticatedHubServerFactory.StartLoopbackAsync(hub =>
        {
            hub.Services.Configure<AccessOptions>(o => o.AccessSweepIntervalSeconds = sweepSeconds);
            if (flowControl != null) hub.Services.AddSingleton<IFlowControlService>(new Rules(flowControl));
        });

    private static async Task<Pair> OpenTransferAsync(LoopbackHost host, TokenSource senderTokens, Func<Task<string>>? senderProvider = null)
    {
        var receiverTokens = new TokenSource("fail-closed-receiver", TimeSpan.FromHours(1));
        var sender = TestClients.Create(null, host.BaseUrl, senderProvider ?? senderTokens.GetAsync,
            configureConnection: o => o.Transports = HttpTransportType.WebSockets);
        var receiver = TestClients.Create(null, host.BaseUrl, receiverTokens.GetAsync,
            configureConnection: o => o.Transports = HttpTransportType.WebSockets);
        return await OpenAsync(host, sender, receiver);
    }

    /// <summary>
    /// With tokens nobody renews, so that nothing but what a test does reaches the reconciler: a
    /// renewal runs it, and the first one of each client may land at any moment after it connects.
    /// </summary>
    private static async Task<Pair> OpenTransferWithStaticTokensAsync(LoopbackHost host)
    {
        var sender = TestClients.Create(null, host.BaseUrl,
            accessToken: StubJwt.Create("fail-closed-sender", DateTime.UtcNow.AddHours(1), AuthenticatedHubServerFactory.DefaultGroup),
            configureConnection: o => o.Transports = HttpTransportType.WebSockets);
        var receiver = TestClients.Create(null, host.BaseUrl,
            accessToken: StubJwt.Create("fail-closed-receiver", DateTime.UtcNow.AddHours(1), AuthenticatedHubServerFactory.DefaultGroup),
            configureConnection: o => o.Transports = HttpTransportType.WebSockets);
        return await OpenAsync(host, sender, receiver);
    }

    private static async Task<Pair> OpenAsync(LoopbackHost host, TransportRClient<string> sender, TransportRClient<string> receiver)
    {

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { receiveHandle.TrySetResult(r); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        var receive = await receiveHandle.Task.WaitAsync(Patience);

        return new Pair(host, sender, receiver, send, receive);
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
    public async Task TokenThatIsNotRenewed_EndsTheTransfer_ClosesTheConnection_AndTheClientComesBackWithANewOne()
    {
        await using var host = await StartHubAsync();
        var store = host.Services.GetRequiredService<IStateStore>();

        // The provider answers once, for the connection, and then fails until the identity provider is back.
        var tokens = new TokenSource("fail-closed-sender", TimeSpan.FromSeconds(4));
        var calls = 0;
        var recovered = false;
        Task<string> Provider() => Interlocked.Increment(ref calls) > 1 && !Volatile.Read(ref recovered)
            ? throw new InvalidOperationException("The identity provider is down.")
            : tokens.GetAsync();

        await using var pair = await OpenTransferAsync(host, tokens, Provider);
        var connection = (await store.GetClientAsync(pair.Sender.ClientId))!.ConnectionId;

        // The token runs out at four seconds, and the sweep sees it within one more.
        await Assert.ThrowsAsync<TransferFailedException>(() => pair.Receive.WaitForCompletionAsync().WaitAsync(Patience));

        // The transfer ended before the connection closed, so the sender it was closed on is told too.
        await Assert.ThrowsAsync<TransferFailedException>(() => pair.Send.CompleteAsync().WaitAsync(Patience));

        var closed = await EventuallyAsync(
            async () => await store.GetClientAsync(pair.Sender.ClientId),
            r => r?.DisconnectedAtUtc != null, Patience);
        Assert.NotNull(closed?.DisconnectedAtUtc);

        // The identity provider is back: the client restarts its connection with a new token and is registered on it.
        Volatile.Write(ref recovered, true);
        var back = await EventuallyAsync(
            async () => await store.GetClientAsync(pair.Sender.ClientId),
            r => r != null && r.ConnectionId != connection && r.DisconnectedAtUtc == null, Patience);

        Assert.NotEqual(connection, back!.ConnectionId);
        Assert.True(back.TokenExpiry > DateTime.UtcNow);
        Assert.NotNull(await pair.Sender.GetReceiversAsync());
    }

    [Fact]
    public async Task RightTakenAwayByTheNewToken_EndsTheActiveTransfer_WithoutClosingTheConnection()
    {
        var allowed = new[] { "senders" };
        await using var host = await AuthenticatedHubServerFactory.StartLoopbackAsync(hub =>
        {
            hub.Services.Configure<AccessOptions>(o => o.AccessSweepIntervalSeconds = 3600);
            hub.Services.AddSingleton<IFlowControlService>(new GroupRules(source => source.Overlaps(allowed)));
        });
        var store = host.Services.GetRequiredService<IStateStore>();

        var senderTokens = new TokenSource("fail-closed-sender", TimeSpan.FromSeconds(4), "senders");
        var receiverTokens = new TokenSource("fail-closed-receiver", TimeSpan.FromHours(1), "receivers");
        await using var sender = TestClients.Create(null, host.BaseUrl, senderTokens.GetAsync,
            configureConnection: o => o.Transports = HttpTransportType.WebSockets);
        await using var receiver = TestClients.Create(null, host.BaseUrl, receiverTokens.GetAsync,
            configureConnection: o => o.Transports = HttpTransportType.WebSockets);
        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { receiveHandle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task.WaitAsync(Patience);
        var connection = (await store.GetClientAsync(sender.ClientId))!.ConnectionId;

        // The identity provider now says the sender is something else. Its next token, at two seconds, carries that.
        senderTokens.Groups = ["bystanders"];

        await Assert.ThrowsAsync<TransferFailedException>(() => receive.WaitForCompletionAsync().WaitAsync(Patience));

        // Its token is good and it holds a group: it may still act, so its connection stands.
        var registration = (await store.GetClientAsync(sender.ClientId))!;
        Assert.Equal(connection, registration.ConnectionId);
        Assert.Null(registration.DisconnectedAtUtc);
        Assert.Equal(["bystanders"], registration.Groups);
    }

    private sealed class GroupRules(Func<IReadOnlySet<string>, bool> allowed) : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => allowed(sourceGroups);
    }

    [Fact]
    public async Task RightWithdrawnByTheHost_IsAppliedByAnExplicitReconcile_WithoutAnySweepOrRenewal()
    {
        var allowed = true;
        await using var host = await StartHubAsync(sweepSeconds: 3600, flowControl: () => Volatile.Read(ref allowed));
        await using var pair = await OpenTransferWithStaticTokensAsync(host);

        Volatile.Write(ref allowed, false);
        await host.Services.GetRequiredService<IAccessReconciler>().ReconcileAllAsync();

        await Assert.ThrowsAsync<TransferFailedException>(() => pair.Receive.WaitForCompletionAsync().WaitAsync(Patience));
    }

    [Fact]
    public async Task RightWithdrawnByTheHost_IsAppliedByTheSweepAlone()
    {
        var allowed = true;
        await using var host = await StartHubAsync(sweepSeconds: 1, flowControl: () => Volatile.Read(ref allowed));
        await using var pair = await OpenTransferWithStaticTokensAsync(host);

        Volatile.Write(ref allowed, false);

        await Assert.ThrowsAsync<TransferFailedException>(() => pair.Receive.WaitForCompletionAsync().WaitAsync(Patience));
    }

    [Fact]
    public async Task DevMode_NothingExpires_NothingIsClosed()
    {
        using var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddDataHub().UseDevMode(o => o.DefaultGroups = ["test"]).Build();
                    services.Configure<AccessOptions>(o => o.AccessSweepIntervalSeconds = 1);
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: false));
                }))
            .Start();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = TestClients.Create(handler, "http://localhost");
        await using var receiver = TestClients.Create(handler, "http://localhost");
        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { receiveHandle.TrySetResult(r); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        var before = (await host.Services.GetRequiredService<IStateStore>().GetClientAsync(sender.ClientId))!.ConnectionId;

        // Three sweeps with nothing to find.
        await Task.Delay(TimeSpan.FromSeconds(3.5));

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task.WaitAsync(Patience);
        var after = (await host.Services.GetRequiredService<IStateStore>().GetClientAsync(sender.ClientId))!;

        Assert.Equal(send.TransferId, receive.TransferId);
        Assert.Equal(before, after.ConnectionId);
        Assert.Null(after.DisconnectedAtUtc);
    }
}

using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using TransportR.Client;
using TransportR.Client.Configuration;
using TransportR.Client.Services;
using TransportR.Hub;
using TransportR.Hub.Access;
using TransportR.Hub.Services;
using TransportR.Identity;
using TransportR.Protocol;
using TransportR.Services;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A host that owns the rights of its clients changes them while the clients run, and wants the hub
/// to take them now. Every scenario here is a client whose stream has outlived the token it opened
/// with: the stream's principal is then older than the registration, which is the state a long-lived
/// client is in. A refresh registers the client on a new stream, from the token of the moment, and
/// nothing on the way may read the old principal.
/// </summary>
public class RegistrationRefreshTests
{
    private const string Sender = "refreshing-node";
    private static readonly TimeSpan OpeningLifetime = TimeSpan.FromSeconds(3);

    // ========== The client ==========

    [Fact]
    public async Task Refresh_TakesTheGroupsTheIdentityHoldsNow_AndAFreshExpiry_OnTheSameRegistration()
    {
        await using var node = await Node.StartAsync(["alpha"]);
        var before = await node.RegistrationAsync();

        node.Rights.Set(Sender, "beta");
        await node.Client.RefreshRegistrationAsync();

        // Read at once: the call returns when the registration is written, not when it was asked for.
        var after = await node.RegistrationAsync();
        Assert.Equal(["beta"], after.Groups.Order());
        Assert.Equal(before.ClientId, after.ClientId);
        Assert.Equal(before.Subject, after.Subject);
        Assert.NotEqual(before.ConnectionId, after.ConnectionId);

        // The old stream closes after the new one registered, and its end must not mark the new one.
        await Task.Delay(200);
        Assert.Null((await node.RegistrationAsync()).DisconnectedAtUtc);
        Assert.True(after.TokenExpiry > before.TokenExpiry, $"the expiry stayed at {after.TokenExpiry:O}");
        Assert.True(after.TokenExpiry > DateTime.UtcNow.AddMinutes(30), $"the expiry is {after.TokenExpiry:O}");
    }

    [Fact]
    public async Task Refresh_NeverLeavesTheRegistrationWithTheExpiryOfTheTokenTheConnectionOpenedWith()
    {
        // The principal of the stream the client holds is that of a token that ran out long ago. A
        // registration written with that expiry is one the next sweep ends the client's transfers for
        // and closes its stream over, however briefly it stays.
        await using var node = await Node.StartAsync(["alpha"]);
        var already = node.Store.Writes.Count;

        node.Rights.Set(Sender, "beta");
        await node.Client.RefreshRegistrationAsync();

        var written = node.Store.Writes.Skip(already).ToList();
        Assert.NotEmpty(written);
        Assert.All(written, w => Assert.True(w.TokenExpiry > DateTime.UtcNow, $"{w.Kind} wrote the expiry {w.TokenExpiry:O}"));
    }

    [Fact]
    public async Task Refresh_LeavesATransferOpenedBeforeItRunning()
    {
        await using var node = await Node.StartAsync(["g"]);
        await using var receiver = node.NewClient("receiver", "g");
        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await receiver.ConnectAsync();

        await using var send = await node.Client.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await handle.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var expected = Enumerable.Range(0, 250).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync()) items.Add(item);
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected.Take(100).ToList());
        node.Rights.Set(Sender, "g", "extra");
        await node.Client.RefreshRegistrationAsync();
        await node.Sweep();
        await send.SendAsync(expected.Skip(100).ToList());
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(["extra", "g"], (await node.RegistrationAsync()).Groups.Order());
    }

    [Fact]
    public async Task Refresh_WhenTheIdentityHoldsNoGroup_IsRefused_AndWritesNothing()
    {
        await using var node = await Node.StartAsync(["alpha"]);
        var before = await node.RegistrationAsync();
        var already = node.Store.Writes.Count;

        node.Rights.Set(Sender);
        await Assert.ThrowsAsync<HubRequestFailedException>(() => node.Client.RefreshRegistrationAsync());

        Assert.Equal(already, node.Store.Writes.Count);
        Assert.Equal(before, await node.RegistrationAsync());
    }

    [Fact]
    public async Task Refresh_WhenTheTokenProviderFails_RaisesItsError_AndWritesNothing()
    {
        await using var node = await Node.StartAsync(["alpha"]);
        var before = await node.RegistrationAsync();
        var already = node.Store.Writes.Count;

        node.Tokens.Failure = new InvalidOperationException("The identity provider is down.");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => node.Client.RefreshRegistrationAsync());

        Assert.Equal("The identity provider is down.", ex.Message);
        Assert.Equal(already, node.Store.Writes.Count);
        Assert.Equal(before, await node.RegistrationAsync());
    }

    [Fact]
    public async Task Refresh_WhenTheHubRefusesTheToken_RaisesTheRefusal_AndLeavesAGoodRegistration()
    {
        await using var node = await Node.StartAsync(["alpha"]);

        // The host's authentication turns a token past its expiry away.
        node.Tokens.NextIsExpired = true;
        await Assert.ThrowsAsync<HttpRequestException>(() => node.Client.RefreshRegistrationAsync());

        var after = await node.RegistrationAsync();
        Assert.True(after.TokenExpiry > DateTime.UtcNow.AddMinutes(30), $"the expiry is {after.TokenExpiry:O}");
    }

    [Fact]
    public async Task Refresh_OnAClientThatNeverConnected_IsAnInvalidOperation()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        await using var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost", new Tokens(Sender).GetAsync);

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.RefreshRegistrationAsync());
    }

    [Fact]
    public async Task Refresh_OnAClientThatDisconnected_IsAnInvalidOperation()
    {
        await using var node = await Node.StartAsync(["alpha"], outlive: false);

        await node.Client.DisconnectAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => node.Client.RefreshRegistrationAsync());
    }

    [Fact]
    public async Task Refresh_WhileTheConnectionIsDown_RegistersTheClientAtOnce()
    {
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        var before = await node.RegistrationAsync();
        node.Rights.Set(Sender, "beta");

        // The client opens a lost stream again after a delay of at least half a second: the refresh
        // comes first.
        await node.CutAsync();
        await node.Client.RefreshRegistrationAsync();

        Assert.True(node.Client.Channel.IsOpen);
        var after = await node.RegistrationAsync();
        Assert.NotEqual(before.ConnectionId, after.ConnectionId);
        Assert.Equal(["beta"], after.Groups.Order());

        // The reopening's own attempt, due within a second of the cut, must find the stream back and
        // open none: a second registration, closed at once, would take the connection over and leave
        // it marked disconnected.
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var settled = await node.RegistrationAsync();
        Assert.Equal(after.ConnectionId, settled.ConnectionId);
        Assert.Null(settled.DisconnectedAtUtc);
    }

    [Fact]
    public async Task Refresh_WhileTheHubRefusesTheConnectionAgain_RaisesTheRefusal_AndTheClientKeepsTrying()
    {
        // A renewal that leaves the identity no group closes its stream, and every attempt to open it
        // again is refused. The host's refresh must say so, not that the client is merely offline.
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        node.Rights.Set(Sender);
        await node.CutAsync();

        await Assert.ThrowsAsync<HubRequestFailedException>(() => node.Client.RefreshRegistrationAsync());
        Assert.False(node.Client.Channel.IsOpen);

        node.Rights.Set(Sender, "beta");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (!node.Client.Channel.IsOpen)
        {
            Assert.True(DateTime.UtcNow < deadline, "the client stopped opening its stream again");
            await Task.Delay(50);
        }

        Assert.Equal(["beta"], (await node.RegistrationAsync()).Groups.Order());
    }

    [Fact]
    public async Task Refresh_WhileTheHubIsOutOfReach_IsAnInvalidOperation()
    {
        // A host keeps a client the hub cannot be reached for: its id and its transfers outlive the cut.
        var rights = new Rights();
        rights.Set(Sender, "alpha");
        var host = await AuthenticatedHubServerFactory.StartLoopbackAsync(configureServices: services =>
        {
            Node.Replacing(rights, new RecordingStateStore())(services);
            services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(1));
        });
        await using var client = TestClients.Create(null, host.BaseUrl, new Tokens(Sender).GetAsync);
        await client.ConnectAsync();

        await host.DisposeAsync();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (client.Channel.IsOpen)
        {
            Assert.True(DateTime.UtcNow < deadline, "the client never saw its stream close");
            await Task.Delay(20);
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => client.RefreshRegistrationAsync());
        Assert.NotNull(ex.InnerException);
    }

    [Fact]
    public async Task Refresh_WhileTheHubDoesNotAnswerTheConnection_EndsWithItsToken()
    {
        // A hub that takes the connection and never registers it: the caller's token is what bounds
        // the refresh, well ahead of the stream's silence limit. The refresh starts before the
        // reopening's own attempt, due half a second after the cut at the earliest, so it is the
        // refresh's opening that the token cuts here, not its wait for that attempt.
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Store.HoldConnects = held.Task;
        try
        {
            await node.CutAsync();

            using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            var clock = System.Diagnostics.Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.Client.RefreshRegistrationAsync(cancel.Token));
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"the refresh outlived its token: {clock.Elapsed}");
        }
        finally
        {
            held.TrySetResult();
        }
    }

    [Fact]
    public async Task AConnectionThatRestarts_RegistersWithTheGroupsOfTheMoment_UnderTheSameSubject()
    {
        // What a host refused a refresh while the connection is down relies on: the client comes
        // back by itself, and with what the identity holds then.
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        var before = await node.RegistrationAsync();

        node.Rights.Set(Sender, "beta");
        await node.CutAsync();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        var after = await node.RegistrationAsync();
        while ((after.ConnectionId == before.ConnectionId || after.DisconnectedAtUtc != null) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
            after = await node.RegistrationAsync();
        }

        Assert.NotEqual(before.ConnectionId, after.ConnectionId);
        Assert.Null(after.DisconnectedAtUtc);
        Assert.Equal(["beta"], after.Groups.Order());
        Assert.Equal(before.Subject, after.Subject);
    }

    [Fact]
    public async Task Refresh_Calls_DoNotInterleaveTheirRegistrationAndTheirPush()
    {
        // Two refreshes each write a registration and then present the token. If one's registration
        // could land between the other's and its push, the push would be answered for a stream that
        // has already been replaced.
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        node.Store.RenewDelay = TimeSpan.FromMilliseconds(400);
        var already = node.Store.Writes.Count;

        var first = node.Client.RefreshRegistrationAsync();
        var second = node.Client.RefreshRegistrationAsync();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(["Connect", "Renewal", "Connect", "Renewal"], node.Store.Writes.Skip(already).Select(w => w.Kind));
    }

    [Fact]
    public async Task Refresh_KeepsThePresenceHeartbeatOutWhileItWrites()
    {
        // A beat that read the registration as unknown just before the refresh would register the
        // client a second time over it. While a refresh is under way no beat is sent at all.
        await using var node = await Node.StartAsync(["alpha"], outlive: false, options => options.PresenceHeartbeatInterval = TimeSpan.FromSeconds(1));
        // Long enough for two ticks of the interval, so one falls inside the refresh wherever the timer stands.
        node.Store.RenewDelay = TimeSpan.FromMilliseconds(2500);
        var already = node.Store.Writes.Count;

        var refresh = node.Client.RefreshRegistrationAsync();
        while (node.Store.Writes.Count == already) await Task.Delay(5);
        var beats = node.Store.LookupsByConnection;
        await refresh.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(beats, node.Store.LookupsByConnection);

        // And they resume once it is done: at the next tick, however late a loaded runner delivers it.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (node.Store.LookupsByConnection <= beats && DateTime.UtcNow < deadline) await Task.Delay(20);
        Assert.True(node.Store.LookupsByConnection > beats, "the presence heartbeat never came back");
    }

    [Fact]
    public async Task Refresh_WhoseTokenPushIsUnderWay_EndsCancelled_WhenTheClientIsDisposed()
    {
        // The push waits on the hub, and the client's disposal is what has to end that wait: the
        // HttpClient it goes over is disposed after it, and would end it as a timeout.
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        node.Store.RenewDelay = TimeSpan.FromSeconds(3);
        var renewals = node.Store.Writes.Count(w => w.Kind == "Renewal");

        var refresh = node.Client.RefreshRegistrationAsync();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (node.Store.Writes.Count(w => w.Kind == "Renewal") == renewals)
        {
            Assert.True(DateTime.UtcNow < deadline, "the refresh never pushed its token");
            await Task.Delay(5);
        }

        await node.Client.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => refresh.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task Refresh_WithoutATokenProvider_RegistersTheClientAgain()
    {
        // Dev mode, or a static token: nothing to renew, and a hub that forgot the client is told again.
        using var host = TestHubServerFactory.CreateHost();
        await using var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost");
        var store = host.Services.GetRequiredService<IStateStore>();

        await client.ConnectAsync(["dev-group"]);
        var before = await store.GetClientAsync(client.ClientId);
        await store.DeleteClientAsync(client.ClientId);

        await client.RefreshRegistrationAsync();

        var after = await store.GetClientAsync(client.ClientId);
        Assert.NotNull(after);
        Assert.Equal(before!.Groups.Order(), after.Groups.Order());
    }

    // ========== The hub ==========

    [Fact]
    public async Task AStreamOfAnotherConnection_RegistersWithTheExpiryOfItsOwnPrincipal()
    {
        // A client that comes back on a new stream is registered from the token that stream opened
        // with, which may well be the shorter one.
        await using var node = await Node.StartAsync(["alpha"], outlive: false);
        var shorter = DateTime.UtcNow.AddMinutes(10);
        using var http = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Get,
            $"{node.Host.BaseUrl}{WireRoutes.ClientBase}/{node.Client.ClientId}/{WireRoutes.EventsSegment}?{ControlPlane.HeartbeatParameter}=15000");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", StubJwt.Create(Sender, shorter, "token-group"));

        using var other = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        Assert.Equal(HttpStatusCode.OK, other.StatusCode);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        var after = await node.RegistrationAsync();
        while (after.TokenExpiry > shorter.AddSeconds(1) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
            after = await node.RegistrationAsync();
        }

        Assert.True(after.TokenExpiry <= shorter.AddSeconds(1), $"the expiry is {after.TokenExpiry:O}");
    }

    // ========== Harness ==========

    /// <summary>Groups per subject, read at every call: the rights document of a host that owns them.</summary>
    private sealed class Rights : IClientIdentityProvider
    {
        private readonly ConcurrentDictionary<string, string[]> _bySubject = new();
        private readonly JwtIdentityProvider _token = new();

        public void Set(string subject, params string[] groups) => _bySubject[subject] = groups;

        public ClientIdentity? ResolveIdentity(ClaimsPrincipal? user, Guid? providedClientId, IEnumerable<string>? providedGroups)
        {
            var identity = _token.ResolveIdentity(user, providedClientId, providedGroups);
            if (identity is null) return null;

            var groups = _bySubject.TryGetValue(identity.Subject ?? "", out var held) ? held : [];
            return identity with { Groups = groups.ToHashSet() };
        }
    }

    /// <summary>
    /// A client's tokens: the first lives <c>opening</c> and is the principal the stream keeps; the
    /// ones after it live an hour.
    /// </summary>
    private sealed class Tokens
    {
        private readonly string _subject;
        private readonly TimeSpan _opening;
        private readonly string[] _openingGroups;
        private readonly string[] _groups;
        private int _calls;

        public Tokens(string subject, TimeSpan? opening = null, string[]? openingGroups = null, string[]? groups = null)
        {
            _subject = subject;
            _opening = opening ?? TimeSpan.FromHours(1);
            _groups = groups ?? ["token-group"];
            _openingGroups = openingGroups ?? _groups;
        }

        public Exception? Failure { get; set; }

        public bool NextIsExpired { get; set; }

        public Task<string> GetAsync()
        {
            if (Failure is { } failure) throw failure;

            var opening = Interlocked.Increment(ref _calls) == 1;
            var lifetime = NextIsExpired ? TimeSpan.FromSeconds(-5)
                : opening ? _opening
                : TimeSpan.FromHours(1);
            NextIsExpired = false;
            return Task.FromResult(StubJwt.Create(_subject, DateTime.UtcNow + lifetime, opening ? _openingGroups : _groups));
        }
    }

    /// <summary>The state store, recording what is written to a registration, in the order it arrives.</summary>
    private sealed class RecordingStateStore : IStateStore
    {
        public sealed record Write(string Kind, DateTime? TokenExpiry);

        private readonly InMemoryStateStore _inner = new();

        public ConcurrentQueue<Write> Writes { get; } = new();

        /// <summary>How long a renewal takes to land, so that whatever else starts meanwhile shows.</summary>
        public TimeSpan RenewDelay { get; set; }

        /// <summary>Lookups by connection: what a presence beat does, and little else here.</summary>
        public int LookupsByConnection => Volatile.Read(ref _lookupsByConnection);

        private int _lookupsByConnection;

        /// <summary>Holds every registration of a stream until it completes: a hub that does not answer.</summary>
        public Task? HoldConnects { get; set; }

        public async Task SaveClientAsync(ClientRegistration client)
        {
            if (HoldConnects is { } held) await held;
            Writes.Enqueue(new Write("Connect", client.TokenExpiry));
            await _inner.SaveClientAsync(client);
        }

        public async Task<ClientRegistration?> RenewClientAsync(Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry)
        {
            Writes.Enqueue(new Write("Renewal", tokenExpiry));
            if (RenewDelay > TimeSpan.Zero) await Task.Delay(RenewDelay);
            return await _inner.RenewClientAsync(clientId, expectedSubject, groups, tokenExpiry);
        }

        public Task<ClientRegistration?> GetClientAsync(Guid clientId) => _inner.GetClientAsync(clientId);
        public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
        {
            Interlocked.Increment(ref _lookupsByConnection);
            return _inner.GetClientByConnectionIdAsync(connectionId);
        }
        public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync() => _inner.GetAllClientsAsync();
        public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc) => _inner.MarkDisconnectedAsync(connectionId, whenUtc);
        public Task DeleteClientAsync(Guid clientId) => _inner.DeleteClientAsync(clientId);
        public Task SaveSessionAsync(TransferSession session) => _inner.SaveSessionAsync(session);
        public Task<TransferSession?> GetSessionAsync(string transferId) => _inner.GetSessionAsync(transferId);
        public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry) => _inner.ExtendSessionExpiryAsync(transferId, newExpiry);
        public Task DeleteSessionAsync(string transferId) => _inner.DeleteSessionAsync(transferId);
    }

    /// <summary>
    /// A hub on a real socket, and a client of it whose stream has outlived its opening token.
    /// </summary>
    private sealed class Node : IAsyncDisposable
    {
        public required LoopbackHost Host { get; init; }
        public required Rights Rights { get; init; }
        public required RecordingStateStore Store { get; init; }
        public required Tokens Tokens { get; init; }
        public required TransportRClient<string> Client { get; init; }

        public static Action<IServiceCollection> Replacing(Rights rights, RecordingStateStore store) =>
            services =>
            {
                services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights));
                services.Replace(ServiceDescriptor.Singleton<IStateStore>(store));
            };

        /// <param name="groups">What the host's rights say of the client.</param>
        public static async Task<Node> StartAsync(
            string[] groups, bool outlive = true, Action<ClientOptions>? options = null)
        {
            var rights = new Rights();
            rights.Set(Sender, groups);
            var store = new RecordingStateStore();
            var host = await AuthenticatedHubServerFactory.StartLoopbackAsync(configureServices: Replacing(rights, store));
            var tokens = new Tokens(Sender, outlive ? OpeningLifetime : null);

            // The client asks its provider again once a token is within the threshold of its expiry:
            // a threshold under the opening lifetime is what keeps the stream's principal the first
            // token, and lets the renewal bring the hour-long one before the first runs out.
            var client = TestClients.Create(null, host.BaseUrl, tokens.GetAsync, o =>
            {
                if (outlive) o.TokenRefreshThreshold = TimeSpan.FromMilliseconds(100);
                options?.Invoke(o);
            });
            var node = new Node { Host = host, Rights = rights, Store = store, Tokens = tokens, Client = client };

            await client.ConnectAsync();

            // The client presents the token it holds as soon as it has connected: wait for that, so
            // that what follows starts from a settled registration.
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while ((await node.RegistrationAsync()).TokenExpiry < DateTime.UtcNow.AddMinutes(30)
                   || !store.Writes.Any(w => w.Kind == "Renewal"))
            {
                Assert.True(DateTime.UtcNow < deadline, "the client never presented its first token");
                await Task.Delay(25);
            }

            if (outlive)
            {
                // The renewal that brought the hour-long token came ahead of the first one's expiry.
                await Task.Delay(TimeSpan.FromSeconds(1.2));
            }

            return node;
        }

        public TransportRClient<string> NewClient(string subject, params string[] groups)
        {
            Rights.Set(subject, groups);
            return TestClients.Create(null, Host.BaseUrl, new Tokens(subject).GetAsync);
        }

        public async Task<ClientRegistration> RegistrationAsync() =>
            (await Store.GetClientAsync(Client.ClientId))!;

        /// <summary>Closes the client's stream from the hub, and returns once the client has seen it go.</summary>
        public async Task CutAsync()
        {
            var connectionId = (await RegistrationAsync()).ConnectionId;
            Assert.True(Host.Services.GetRequiredService<ClientStreams>().TryClose(connectionId));

            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Client.Channel.IsOpen)
            {
                Assert.True(DateTime.UtcNow < deadline, "the client never saw its stream close");
                await Task.Delay(5);
            }
        }

        /// <summary>The access sweep, now rather than at its next tick.</summary>
        public Task Sweep() => Host.Services.GetRequiredService<IAccessReconciler>().ReconcileAllAsync();

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            await Host.DisposeAsync();
        }
    }
}

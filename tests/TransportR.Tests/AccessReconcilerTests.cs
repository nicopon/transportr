using System.Security.Claims;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Services;

namespace TransportR.Tests;

/// <summary>
/// What a pass over the registrations does with what they say, on a clock the test moves: the
/// order it acts in, what it leaves alone, and which transfers it judges.
/// </summary>
public class AccessReconcilerTests
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private sealed class Rules(Func<bool> allowed) : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => allowed();
    }

    private sealed class Journal
    {
        public List<string> Events { get; } = [];
    }

    private sealed class RecordingTerminator(Journal journal) : ITransferTerminator
    {
        public Task TerminateAsync(string transferId, CancellationToken cancellationToken = default)
        {
            journal.Events.Add("terminate " + transferId);
            return Task.CompletedTask;
        }

    }

    private sealed class FakeContext(string connectionId, Journal journal) : HubCallerContext
    {
        public override string ConnectionId { get; } = connectionId;
        public override string? UserIdentifier => null;
        public override ClaimsPrincipal? User => null;
        public override IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();
        public override IFeatureCollection Features { get; } = new FeatureCollection();
        public override CancellationToken ConnectionAborted => CancellationToken.None;

        public override void Abort() => journal.Events.Add("close " + ConnectionId);
    }

    private sealed class Harness
    {
        public FakeTimeProvider Clock { get; } = new(Start);
        public Journal Journal { get; } = new();
        public InMemoryStateStore Store { get; }
        public TransferManager Transfers { get; }
        public ConnectionRegistry Connections { get; } = new();
        public AccessReconciler Reconciler { get; }

        public Harness(Func<bool>? flowControl = null)
        {
            Store = new InMemoryStateStore(timeProvider: Clock);
            Transfers = new TransferManager(Store, Clock);
            var policy = new AccessPolicy(
                Options.Create(new AccessOptions()), Clock, flowControl == null ? null : new Rules(flowControl));
            Reconciler = new AccessReconciler(
                Store, Transfers, new RecordingTerminator(Journal), policy, Connections, NullLogger<AccessReconciler>.Instance);
        }

        public async Task<Guid> RegisterAsync(string name, TimeSpan? lifetime, bool connected = true, params string[] groups)
        {
            var id = Guid.NewGuid();
            var connectionId = "conn-" + name;
            await Store.SaveClientAsync(new ClientRegistration(
                id, connectionId, (groups.Length == 0 ? ["g"] : groups).ToHashSet(), null,
                lifetime is { } l ? Clock.GetUtcNow().UtcDateTime + l : null, Clock.GetUtcNow().UtcDateTime, Subject: name));
            if (connected) Connections.Add(new FakeContext(connectionId, Journal));
            return id;
        }
    }

    [Fact]
    public async Task ExpiredClient_HasItsTransfersEnded_BeforeItsConnectionIsClosed()
    {
        var harness = new Harness();
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromSeconds(10));
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromHours(1));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);

        harness.Clock.Advance(TimeSpan.FromSeconds(11));
        await harness.Reconciler.ReconcileAllAsync();

        Assert.Equal(["terminate t1", "close conn-sender"], harness.Journal.Events);
    }

    [Fact]
    public async Task ClientWhoseTokenHasNotRunOut_IsLeftAlone()
    {
        var harness = new Harness();
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromSeconds(10));
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromSeconds(10));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);

        harness.Clock.Advance(TimeSpan.FromSeconds(9));
        await harness.Reconciler.ReconcileAllAsync();

        Assert.Empty(harness.Journal.Events);
    }

    [Fact]
    public async Task ClientWithoutGroups_IsClosedLikeAnExpiredOne()
    {
        var harness = new Harness();
        var client = await harness.RegisterAsync("client", TimeSpan.FromHours(1));
        var registered = (await harness.Store.GetClientAsync(client))!;
        await harness.Store.RenewClientAsync(client, "client", new HashSet<string>(), registered.TokenExpiry);

        await harness.Reconciler.ReconcileAllAsync();

        Assert.Equal(["close conn-client"], harness.Journal.Events);
    }

    [Fact]
    public async Task TransferOfTwoExpiredClients_IsEndedOnce()
    {
        var harness = new Harness();
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromSeconds(10));
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromSeconds(10));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);

        harness.Clock.Advance(TimeSpan.FromSeconds(11));
        await harness.Reconciler.ReconcileAllAsync();

        Assert.Single(harness.Journal.Events, e => e == "terminate t1");
        Assert.Contains("close conn-sender", harness.Journal.Events);
        Assert.Contains("close conn-receiver", harness.Journal.Events);
    }

    [Fact]
    public async Task TransferWhosePairTheServiceNoLongerAllows_IsEnded_AndNoConnectionIsClosed()
    {
        var allowed = true;
        var harness = new Harness(() => allowed);
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromHours(1));
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromHours(1));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);

        await harness.Reconciler.ReconcileAllAsync();
        Assert.Empty(harness.Journal.Events);

        allowed = false;
        await harness.Reconciler.ReconcileAllAsync();

        Assert.Equal(["terminate t1"], harness.Journal.Events);
    }

    [Fact]
    public async Task FailingFlowControlService_EndsNothing_AndDoesNotStopThePass()
    {
        var harness = new Harness(() => throw new InvalidOperationException("The policy store is unreachable."));
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromHours(1));
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromHours(1));
        var expired = await harness.RegisterAsync("expired", TimeSpan.FromSeconds(-1));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);

        await harness.Reconciler.ReconcileAllAsync();

        // An error is not a verdict on t1; the client whose token ran out is still closed.
        Assert.Equal(["close conn-expired"], harness.Journal.Events);
    }

    [Fact]
    public async Task DisconnectedClientInItsGrace_WhoseTokenRanOut_HasItsTransfersEnded()
    {
        // The connection is already gone, and the data plane may not be: the transfers go all the same.
        var harness = new Harness();
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromSeconds(10), connected: false);
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromHours(1));
        await harness.Store.MarkDisconnectedAsync("conn-sender", harness.Clock.GetUtcNow().UtcDateTime);
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);

        harness.Clock.Advance(TimeSpan.FromSeconds(11));
        await harness.Reconciler.ReconcileAllAsync();

        Assert.Equal(["terminate t1"], harness.Journal.Events);
    }

    [Fact]
    public async Task TransferWithAParticipantTheStoreNoLongerKnows_IsLeftToTheInactivityCheck()
    {
        var harness = new Harness();
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromHours(1));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, Guid.NewGuid());

        await harness.Reconciler.ReconcileAllAsync();

        Assert.Empty(harness.Journal.Events);
    }

    [Fact]
    public async Task ReconcilingOneClient_JudgesItsTransfersAndNobodyElses()
    {
        var harness = new Harness();
        var sender = await harness.RegisterAsync("sender", TimeSpan.FromSeconds(10));
        var receiver = await harness.RegisterAsync("receiver", TimeSpan.FromHours(1));
        var otherSender = await harness.RegisterAsync("other-sender", TimeSpan.FromSeconds(10));
        var otherReceiver = await harness.RegisterAsync("other-receiver", TimeSpan.FromHours(1));
        await harness.Transfers.RegisterParticipantsAsync("t1", sender, receiver);
        await harness.Transfers.RegisterParticipantsAsync("t2", otherSender, otherReceiver);

        harness.Clock.Advance(TimeSpan.FromSeconds(11));
        await harness.Reconciler.ReconcileAsync(sender);

        Assert.Equal(["terminate t1", "close conn-sender"], harness.Journal.Events);
    }

    [Fact]
    public async Task ReconcilingAnUnknownClient_DoesNothing()
    {
        var harness = new Harness();

        await harness.Reconciler.ReconcileAsync(Guid.NewGuid());

        Assert.Empty(harness.Journal.Events);
    }

    [Fact]
    public async Task Sweep_RunsThePassOnEveryTick_AndSurvivesAFailedOne()
    {
        var clock = new FakeTimeProvider(Start);
        var passes = 0;
        var reconciler = new CountingReconciler(() =>
        {
            if (Interlocked.Increment(ref passes) == 1) throw new InvalidOperationException("The store is down.");
        });
        var sweep = new AccessSweepService(
            reconciler, Options.Create(new AccessOptions { AccessSweepIntervalSeconds = 5 }), clock, NullLogger<AccessSweepService>.Instance);

        await sweep.StartAsync(CancellationToken.None);

        // The timer may be created after the first Advance, so the clock moves in steps.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (Volatile.Read(ref passes) < 3 && DateTime.UtcNow < deadline)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(10);
        }

        await sweep.StopAsync(CancellationToken.None);
        Assert.True(passes >= 3, $"{passes} passes.");
    }

    private sealed class CountingReconciler(Action onPass) : IAccessReconciler
    {
        public Task ReconcileAsync(Guid clientId, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task ReconcileAllAsync(CancellationToken cancellationToken = default)
        {
            onPass();
            return Task.CompletedTask;
        }
    }
}

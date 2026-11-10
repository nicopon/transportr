using System.Collections.Concurrent;
using System.Security.Claims;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.Abstractions;
using TransportR.Client.Services;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A host that owns the rights of its clients and runs the hub in its own process can withdraw a
/// right from the clients' registrations without any client taking part, through two interfaces
/// the hub already exposes: the state store, to write what a subject now holds, and the access
/// reconciler, to apply it to what is running. This is the recipe, played end to end on a socket
/// with nothing else acting (no sweep, no renewal), so that what it does is what the documentation
/// can say it does.
/// </summary>
public class HostRevocationTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task WithdrawingEveryGroup_EndsTheTransfers_ClosesTheConnection_AndTheClientStaysOutWhileItsIdentityHoldsNone()
    {
        await using var node = await Node.StartAsync(canSendTo: (_, _) => true);

        node.Rights.Set(Node.SenderSubject);
        await node.WithdrawAsync(Node.SenderSubject);

        await Assert.ThrowsAsync<TransferFailedException>(() => node.Receive.WaitForCompletionAsync().WaitAsync(Patience));
        await EventuallyAsync(() => !node.Sender.Channel.IsOpen, Patience);
        Assert.Empty((await node.RegistrationAsync()).Groups);

        // The client opens its stream again, which asks to be registered; its identity still holds no
        // group, so the hub refuses, and it stays out of discovery.
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.False(node.Sender.Channel.IsOpen);
        Assert.Empty((await node.RegistrationAsync()).Groups);
        Assert.DoesNotContain(await node.Receiver.GetReceiversAsync(), c => c.ClientId == node.Sender.ClientId);
    }

    [Fact]
    public async Task WithdrawingAGroup_EndsTheTransfersTheNewGroupsRefuse_AndKeepsTheConnection()
    {
        await using var node = await Node.StartAsync(canSendTo: (source, _) => source.Contains("senders"));

        node.Rights.Set(Node.SenderSubject, "bystanders");
        await node.WithdrawAsync(Node.SenderSubject, "bystanders");

        await Assert.ThrowsAsync<TransferFailedException>(() => node.Receive.WaitForCompletionAsync().WaitAsync(Patience));
        var registration = await node.RegistrationAsync();
        Assert.Equal(["bystanders"], registration.Groups);
        Assert.Equal(node.ConnectionId, registration.ConnectionId);

        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.True(node.Sender.Channel.IsOpen, "the stream was closed");
        Assert.Equal(node.ConnectionId, (await node.RegistrationAsync()).ConnectionId);
    }

    private static async Task EventuallyAsync(Func<bool> done, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!done() && DateTime.UtcNow < deadline) await Task.Delay(50);
        Assert.True(done(), "the condition never held");
    }

    /// <summary>Groups per subject, read at every registration: the rights document of a host that owns them.</summary>
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

    private sealed class Rules(Func<IReadOnlySet<string>, IReadOnlySet<string>, bool> canSendTo) : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) => canSendTo(sourceGroups, targetGroups);
    }

    /// <summary>A sender with a transfer open to a receiver, on a hub where only the host's own calls judge anything.</summary>
    private sealed class Node : IAsyncDisposable
    {
        public const string SenderSubject = "revoked-sender";
        private const string ReceiverSubject = "revoked-receiver";

        public required LoopbackHost Host { get; init; }
        public required Rights Rights { get; init; }
        public required TransportRClient<string> Sender { get; init; }
        public required TransportRClient<string> Receiver { get; init; }
        public required Send<string> Send { get; init; }
        public required Receive<string> Receive { get; init; }
        public required string ConnectionId { get; init; }

        public static async Task<Node> StartAsync(Func<IReadOnlySet<string>, IReadOnlySet<string>, bool> canSendTo)
        {
            var rights = new Rights();
            rights.Set(SenderSubject, "senders");
            rights.Set(ReceiverSubject, "receivers");

            // An hour between sweeps, and tokens nobody renews: the host's calls are the only judge.
            var host = await AuthenticatedHubServerFactory.StartLoopbackAsync(
                hub =>
                {
                    hub.Services.Configure<AccessOptions>(o => o.AccessSweepIntervalSeconds = 3600);
                    hub.Services.AddSingleton<IFlowControlService>(new Rules(canSendTo));
                },
                services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)));

            var sender = TestClients.Create(null, host.BaseUrl,
                accessToken: StubJwt.Create(SenderSubject, DateTime.UtcNow.AddHours(1), "unused"));
            var receiver = TestClients.Create(null, host.BaseUrl,
                accessToken: StubJwt.Create(ReceiverSubject, DateTime.UtcNow.AddHours(1), "unused"));

            var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.OnTransferStarted += r => { receiveHandle.TrySetResult(r); return Task.CompletedTask; };
            await sender.ConnectAsync();
            await receiver.ConnectAsync();
            var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
            var receive = await receiveHandle.Task.WaitAsync(Patience);

            var store = host.Services.GetRequiredService<IStateStore>();
            return new Node
            {
                Host = host,
                Rights = rights,
                Sender = sender,
                Receiver = receiver,
                Send = send,
                Receive = receive,
                ConnectionId = (await store.GetClientAsync(sender.ClientId))!.ConnectionId,
            };
        }

        /// <summary>
        /// What a host does, with nothing but what the hub exposes: write the groups the subject now
        /// holds into each of its registrations, then have the hub judge what they are running.
        /// </summary>
        public async Task WithdrawAsync(string subject, params string[] groups)
        {
            var store = Host.Services.GetRequiredService<IStateStore>();
            var reconciler = Host.Services.GetRequiredService<IAccessReconciler>();

            foreach (var registration in (await store.GetAllClientsAsync()).Where(c => c.Subject == subject))
            {
                await store.RenewClientAsync(registration.ClientId, subject, groups.ToHashSet(), registration.TokenExpiry);
                await reconciler.ReconcileAsync(registration.ClientId);
            }
        }

        public async Task<ClientRegistration> RegistrationAsync() =>
            (await Host.Services.GetRequiredService<IStateStore>().GetClientAsync(Sender.ClientId))!;

        public async ValueTask DisposeAsync()
        {
            await Send.DisposeAsync();
            await Receive.DisposeAsync();
            await Sender.DisposeAsync();
            await Receiver.DisposeAsync();
            await Host.DisposeAsync();
        }
    }
}

using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The route a participant uses to end its transfer when the Abort it wrote cannot reach the wire:
/// behind data nobody reads, the stream that carries it is full, and a request of its own is the only
/// way left to tell the hub. It is an HTTP request, authorized as the data streams are, and it ends
/// the transfer the way a host terminating a run does.
/// </summary>
public class AbortRouteTests
{
    private const string TransferId = "transfer-1";
    private const string SenderSubject = "sender-principal";
    private const string ReceiverSubject = "receiver-principal";

    private static readonly Guid SenderId = Guid.NewGuid();
    private static readonly Guid ReceiverId = Guid.NewGuid();

    [Theory]
    [InlineData(SenderSubject)]
    [InlineData(ReceiverSubject)]
    public async Task AParticipant_EndsTheTransfer_AndBothAreTold(string subject)
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, subject);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.Null(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Equal(["conn-sender", "conn-receiver"], notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task AStranger_IsRefusedAsNotFound_AndTheTransferSurvives()
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, "someone-else");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task AnUnknownTransfer_IsRefusedAsNotFound()
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, "no-such-transfer", SenderSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task AnAnonymousCaller_IsChallenged()
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, subject: null);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
    }

    [Fact]
    public async Task AReplicaThatHoldsNoBridgeOfTheTransfer_RefusesIt_AndLeavesTheTransferAlone()
    {
        // Behind a proxy with affinity, a request that lost its cookie reaches a replica that knows the
        // transfer from the shared state but never carried its streams. Acting there would delete the
        // shared state and tell nobody, because the participants' connections are on the other replica.
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier, holdsTheBridges: false);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, SenderSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task ATransferThatEndedWell_CannotBeAborted()
    {
        // A sender whose Abort did reach the wire a moment after the timeout, or whose transfer was
        // retired for any other reason, meets a transfer the hub no longer knows.
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        await host.Services.GetRequiredService<ITransferTerminator>().RetireAsync(TransferId);

        using var response = await AbortAsync(client, TransferId, SenderSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    private static async Task<IHost> CreateHostWithTransferAsync(RecordingNotifier notifier, bool holdsTheBridges = true)
    {
        var host = AuthenticatedHubServerFactory.CreateHost(configureServices: services =>
            services.AddSingleton<ITransferNotifier>(notifier));

        var store = host.Services.GetRequiredService<IStateStore>();
        await store.SaveClientAsync(new ClientRegistration(SenderId, "conn-sender", ["test"], null, null, DateTime.UtcNow, Subject: SenderSubject));
        await store.SaveClientAsync(new ClientRegistration(ReceiverId, "conn-receiver", ["test"], null, null, DateTime.UtcNow, Subject: ReceiverSubject));
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(TransferId, SenderId, ReceiverId);

        if (holdsTheBridges)
        {
            var bridges = host.Services.GetRequiredService<IBridgeManager>();
            bridges.GetOrCreate(IBridgeManager.KeyFor(TransferId, ConnectionType.Data));
            bridges.GetOrCreate(IBridgeManager.KeyFor(TransferId, ConnectionType.Control));
        }

        return host;
    }

    private static Task<HttpResponseMessage> AbortAsync(HttpClient client, string transferId, string? subject)
    {
        var request = new HttpRequestMessage(
            HttpMethod.Post, $"http://localhost{WireRoutes.StreamBase}/{transferId}/{WireRoutes.AbortSegment}");

        if (subject != null)
        {
            request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {subject}");
        }

        return client.SendAsync(request);
    }

    private sealed class RecordingNotifier : ITransferNotifier
    {
        private readonly List<string> _notified = [];

        public IReadOnlyList<string> NotifiedConnectionIds
        {
            get { lock (_notified) return [.. _notified]; }
        }

        public Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default)
        {
            lock (_notified) _notified.Add(connectionId);
            return Task.CompletedTask;
        }
    }
}
